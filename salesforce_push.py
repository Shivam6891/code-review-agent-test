import json
import boto3
import logging
import traceback
import re
from botocore.exceptions import ClientError
from urllib.parse import urlparse, unquote, quote

# ============================================================
# AWS CONFIG
# ============================================================
AWS_REGION            = "us-east-1"
AWS_ACCESS_KEY_ID     = "YOUR_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "YOUR_SECRET_ACCESS_KEY"
DYNAMODB_TABLE        = "DocInfo-Dev"

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)
dynamodb      = session.resource("dynamodb")
table         = dynamodb.Table(DYNAMODB_TABLE)
s3            = session.client("s3")
lambda_client = session.client("lambda")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ============================================================
# CONSTANTS
# ============================================================

CONTENT_TYPE_MAP = {
    "pdf":  "application/pdf",
    "png":  "image/png",
    "jpg":  "image/jpeg",
    "jpeg": "image/jpeg",
    "tiff": "image/tiff",
    "tif":  "image/tiff",
}

DOC_SUBTYPE_MAP = {
    "Regular Invoice": "Regular",
    "Utility Invoice": "Utility",
    "Credit Memo":     "Credit Memo",
    "CreditMemo":      "Credit Memo",
}

# These errors mean classification could not process the doc —
# only PATCH Document_Manager__c, never create Error_Log__c
SILENT_ERROR_PHRASES = (
    "unclassified",
    "all pages failed",
    "some pages failed",
    "closed:",
    "failed to classify excel file",
    "failed to classify csv file",
)


# ============================================================
# HELPERS
# ============================================================

BODY_JSON_MAX_LEN = 131000

def split_body_json(text, max_len=BODY_JSON_MAX_LEN):
    """
    Split a JSON string across Body_JSON__c / Body_JSON2__c to respect
    Salesforce field limits. Returns (part1, part2_or_None).
    part1 is always <= max_len. part2 holds the remainder (also capped
    at max_len — if the remainder itself exceeds max_len, it is hard
    truncated and the rest is dropped).
    """
    if text is None:
        return "", None
    if len(text) <= max_len:
        return text, None
    part1 = text[:max_len]
    part2 = text[max_len:max_len * 2]
    return part1, part2

ERROR_MSG_MAX_LEN = 255

def truncate_error_msg(msg):
    return (msg or "")[:ERROR_MSG_MAX_LEN]

def get_secret(secret_name):
    client   = boto3.client("secretsmanager", region_name=AWS_REGION)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])


def parse_s3_url(url):
    parsed = urlparse(url)
    bucket = parsed.hostname.split(".")[0]
    key = unquote(parsed.path.lstrip("/"))
    if parsed.fragment:
        key += "#" + unquote(parsed.fragment)
    return bucket, key


def copy_s3_file(source_s3_url, child_doc_id, file_name, dest_bucket, aws_access_key=None, aws_secret_key=None):
    src_bucket, src_key = parse_s3_url(source_s3_url)
    dest_key            = f"Document_Manager__c/{child_doc_id}/{file_name}"

    if aws_access_key and aws_secret_key:
        s3_client = boto3.client(
            "s3",
            region_name           = AWS_REGION,
            aws_access_key_id     = aws_access_key,
            aws_secret_access_key = aws_secret_key,
        )
    else:
        s3_client = s3

    logger.info(f"[S3-COPY] {src_bucket}/{src_key} → {dest_bucket}/{dest_key}")
    s3_client.copy_object(
        CopySource  = {"Bucket": src_bucket, "Key": src_key},
        Bucket      = dest_bucket,
        Key         = dest_key,
        ContentType = CONTENT_TYPE_MAP.get(
            file_name.rsplit(".", 1)[-1].lower(), "application/octet-stream"
        ),
    )
    updated_url = f"https://{dest_bucket}.s3.amazonaws.com/{quote(dest_key, safe='/')}"
    logger.info(f"[S3-COPY] New URL: {updated_url}")
    return updated_url




# ============================================================
# SALESFORCE CALLERS
# ============================================================

def call_sf_lambda(org_id, object_name, object_data, namespace, method="POST", record_id=None):
    """
    Invoke the salesforce_token Lambda to create or update a Salesforce record.
    Returns the Salesforce record ID on success.
    """
    payload = {
        "orgId":      org_id,
        "objectName": object_name,
        "objectData": object_data,
        "nameSpace":  namespace,
        "method":     method,
    }
    if record_id:
        payload["recordId"] = record_id

    logger.info(f"[SF-INVOKE] object={object_name} method={method} payload={json.dumps(payload)}")

    response         = lambda_client.invoke(
        FunctionName   = "salesforce_token_dev",
        InvocationType = "RequestResponse",
        Payload        = json.dumps(payload),
    )
    response_payload = json.load(response["Payload"])
    logger.info(f"[SF-RESPONSE] {json.dumps(response_payload)}")

    if response_payload.get("statusCode") not in (200, 201):
        body_str = response_payload.get("body", "")
        if "STRING_TOO_LONG" in body_str:
            # NOTE: proactive split via split_body_json() should prevent this now.
            # Retry-on-truncate logic intentionally not wired up — log and fail
            # instead of silently retrying with stale truncate logic.
            logger.error(f"[SF-STRING-TOO-LONG] object={object_name} — proactive split should have prevented this. body={body_str}")
        raise Exception(f"SF call failed for {object_name}: {body_str}")

    body = json.loads(response_payload["body"])
    return body["sfResponse"]["id"]


def call_sf_lambda_parent(org_id, object_name, object_data, namespace, method="PATCH", record_id=None):
    """
    Invoke the salesforce_token Lambda for parent-level updates.
    Does not return a record ID (used for PATCH / fire-and-check).
    """
    payload = {
        "orgId":      org_id,
        "objectName": object_name,
        "objectData": object_data,
        "nameSpace":  namespace,
        "method":     method,
    }
    if record_id:
        payload["recordId"] = record_id

    logger.info(f"[SF-INVOKE-PARENT] object={object_name} method={method} payload={json.dumps(payload)}")

    response         = lambda_client.invoke(
        FunctionName   = "salesforce_token_dev",
        InvocationType = "RequestResponse",
        Payload        = json.dumps(payload),
    )
    response_payload = json.load(response["Payload"])
    logger.info(f"[SF-RESPONSE-PARENT] {json.dumps(response_payload)}")

    if response_payload.get("statusCode") not in (200, 201):
        raise Exception(f"SF parent update failed for {object_name}: {response_payload.get('body')}")


# ============================================================
# CORE PROCESSING HELPERS
# ============================================================

def is_silent_error(error_msg):
    """
    Returns True if the error is a known classification failure that should
    NOT create an Error_Log__c record in Salesforce.
    Silent errors: Unclassified, All pages failed, Some pages failed.
    """
    lower = error_msg.lower()
    return any(phrase in lower for phrase in SILENT_ERROR_PHRASES)


def process_child_document(org_id, doc_id, parent_id, message, ns, ns_field, dest_bucket, file_text, file_name, file_ext, doc_sub_type, child_doc_id, parent_error_msg, aws_access_key=None, aws_secret_key=None):
    """
    Handles the json_url truthy branch:
    - If child already exists in SF  → PATCH Document_Manager__c
    - If child is new                → POST Document_Manager__c + POST Amazon_S3_Attachment__c
    In both cases, also PATCH parent Document_Manager__c with page count + errorMsg if any.
    """
    s3_url = message["s3Url"]

    if child_doc_id:
        updated_s3_url = copy_s3_file(s3_url, child_doc_id, file_name, dest_bucket, aws_access_key, aws_secret_key)

        body1, body2 = split_body_json(file_text)

        dm_update_data = {
            ns_field("Body_JSON__c"): body1,
            ns_field("File_URL__c"): updated_s3_url,
            ns_field("Page_Count__c"): message.get("pageCount", 0),
            ns_field("Document_Sub_Type__c"): doc_sub_type,
        }

        # Only send Body_JSON2__c if it actually contains data
        if body2 is not None:
            dm_update_data[ns_field("Body_JSON2__c")] = body2


        call_sf_lambda(
            org_id,
            ns_field("Document_Manager__c"),
            dm_update_data,
            ns,
            method="PATCH",
            record_id=child_doc_id,
        )
        logger.info(f"[DM-UPDATED] child_doc_id={child_doc_id}")


        table.update_item(
            Key                       = {"orgId": org_id, "docId": doc_id},
            UpdateExpression          = "SET SentToSalesforce=:sf",
            ExpressionAttributeValues = {":sf": "Processed"},
        )

    else:
        body1, body2 = split_body_json(file_text)

        dm_data = {
            ns_field("Body_JSON__c"): body1,
            ns_field("Parent_Document__c"): parent_id,
            ns_field("Document_Type__c"): message["docType"],
            ns_field("Document_Sub_Type__c"): doc_sub_type,
            ns_field("File_Name__c"): file_name,
            ns_field("Status__c"): "Analysis Complete",
            ns_field("Error_Message__c"): parent_error_msg,
            ns_field("Page_Count__c"): message.get("pageCount", 0),
        }

        # Only add Body_JSON2__c when the JSON exceeds 131000 characters
        if body2 is not None:
            dm_data[ns_field("Body_JSON2__c")] = body2
        sf_dm_id = call_sf_lambda(org_id, ns_field("Document_Manager__c"), dm_data, ns)
        logger.info(f"[DM-CREATED] sf_dm_id={sf_dm_id}")

        updated_s3_url = copy_s3_file(s3_url, sf_dm_id, file_name, dest_bucket, aws_access_key, aws_secret_key)

        # Update File_URL__c now that we have the final S3 path
        call_sf_lambda(
            org_id,
            ns_field("Document_Manager__c"),
            {ns_field("File_URL__c"): updated_s3_url},
            ns,
            method    = "PATCH",
            record_id = sf_dm_id,
        )

        # Create S3 Attachment record
        att_data = {
            ns_field("Related_To_ID__c"):  sf_dm_id,
            ns_field("File_Size__c"):      len(file_text),
            ns_field("File_Extension__c"): file_ext,
            ns_field("File_URL__c"):       updated_s3_url,
            ns_field("File_Name__c"):      file_name,
        }
        att_id = call_sf_lambda(org_id, ns_field("Amazon_S3_Attachment__c"), att_data, ns)
        logger.info(f"[ATT-CREATED] att_id={att_id}")

        table.update_item(
            Key                       = {"orgId": org_id, "docId": doc_id},
            UpdateExpression          = "SET childDocId=:cid, SentToSalesforce=:sf",
            ExpressionAttributeValues = {":cid": sf_dm_id, ":sf": "Processed"},
        )
        logger.info(f"[DDB-UPDATE] docId={doc_id} sf_dm_id={sf_dm_id}")

    # ── Always PATCH parent Document_Manager__c with page count + errorMsg ──
    try:
        parent_ddb        = table.get_item(Key={"orgId": org_id, "docId": parent_id}).get("Item", {})
        parent_page_count = int(parent_ddb.get("pageCount", 0))
        parent_error = truncate_error_msg(parent_error_msg)

        parent_dm_data = {ns_field("Page_Count__c"): parent_page_count}

        if parent_error:
            parent_dm_data[ns_field("Status__c")]        = "Closed"
            parent_dm_data[ns_field("Error_Message__c")] = parent_error

        call_sf_lambda(
            org_id,
            ns_field("Document_Manager__c"),
            parent_dm_data,
            ns,
            method    = "PATCH",
            record_id = parent_id,
        )
        logger.info(f"[DM-PARENT-UPDATED] parent={parent_id} pageCount={parent_page_count} errorMsg='{parent_error}'")

    except Exception as pc_err:
        logger.warning(f"[DM-PARENT-UPDATED] Failed: {pc_err}")


def process_parent_error(org_id, doc_id, parent_id, message, ns, ns_field, parent_error_msg):
    """
    Handles the json_url falsy branch — parent-level error/failure.
    Always  : PATCH Document_Manager__c on parent (Status=Closed, pageCount, errorMsg)
    Only if real error (not silent): POST Error_Log__c
    Silent errors (no Error_Log__c): Unclassified, All pages failed, Some pages failed
    """
    parent_ddb        = table.get_item(Key={"orgId": org_id, "docId": parent_id}).get("Item", {})
    parent_page_count = int(parent_ddb.get("pageCount", 0))

    # ── Always PATCH parent Document_Manager__c ──
    doc_subtype = message.get("docSubtype", "")

    call_sf_lambda(
        org_id,
        ns_field("Document_Manager__c"),
        {
            ns_field("Page_Count__c"):         parent_page_count,
            ns_field("Status__c"):             "Closed",
            ns_field("Error_Message__c"):      parent_error_msg,
            ns_field("Document_Sub_Type__c"):  doc_subtype,
        },
        ns,
        method    = "PATCH",
        record_id = parent_id,
    )
    logger.info(f"[DM-PARENT-UPDATED] parent={parent_id} Status=Closed pageCount={parent_page_count}")

    # ── Create Error_Log__c only for real/unexpected errors ──
    if parent_error_msg and not is_silent_error(parent_error_msg):
        call_sf_lambda_parent(
            org_id,
            ns_field("Error_Log__c"),
            {
                ns_field("Related_To_Id__c"):     parent_id,
                ns_field("Error_Message__c"):     parent_error_msg,
                ns_field("Exception_Type__c"):    "Document Manager",
                ns_field("Class_Name__c"):        "Python Scheduler",
                ns_field("Method_Name__c"):       "Document Processing",
                ns_field("Stack_Trace__c"):       "",
                ns_field("Salesforce_Limits__c"): "Salesforce Limit",
                ns_field("Line_Number__c"):       1,
            },
            ns,
            method    = "POST",
            record_id = None,
        )
        logger.info(f"[ERROR-LOG-CREATED] Error_Log__c created for error: '{parent_error_msg}'")
    else:
        logger.info(f"[ERROR-LOG-SKIPPED] Silent error — no Error_Log__c created. msg='{parent_error_msg}'")

    # ── Mark doc as Processed in DDB ──
    table.update_item(
        Key                       = {"orgId": org_id, "docId": doc_id},
        UpdateExpression          = "SET SentToSalesforce=:sf",
        ExpressionAttributeValues = {":sf": "Processed"},
    )
    logger.info(f"[DDB-UPDATE] docId={doc_id} marked SentToSalesforce=Processed")


# ============================================================
# LAMBDA HANDLER
# ============================================================

def lambda_handler(event, context):
    logger.info("========== SF PUSH LAMBDA START ==========")

    for record in event.get("Records", []):
        try:
            message  = json.loads(record["body"])
            logger.info(f"[MESSAGE] {json.dumps(message)}")

            # ── Parse minimal fields needed for the claim ──
            org_id = message["orgId"]
            doc_id = message["docId"]

            # ── ATOMIC CLAIM — do this IMMEDIATELY, before any other work ──
            # Only proceed if SentToSalesforce is currently "Draft".
            # Whoever wins this flips it to "InProcess" right here.
            try:
                table.update_item(
                    Key={"orgId": org_id, "docId": doc_id},
                    UpdateExpression="SET SentToSalesforce=:inprocess",
                    ConditionExpression="SentToSalesforce=:draft",
                    ExpressionAttributeValues={":inprocess": "InProcess", ":draft": "Draft"},
                )
                logger.info(f"[CLAIM-WON] {doc_id} → SentToSalesforce set to InProcess")
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    logger.warning(f"[CLAIM-LOST] {doc_id} — SentToSalesforce is not Draft, skipping this delivery")
                    continue
                raise

            # ── Everything below this line only runs for the winner ──
            parent_id     = message["parentId"]
            s3_url        = message["s3Url"]
            json_url      = message["jsonUrl"]
            doc_type      = message["docType"]
            file_name     = message["fileName"]
            namespace     = message.get("nameSpace", "")
            doc_sub_type  = DOC_SUBTYPE_MAP.get(message.get("docSubType", ""), message.get("docSubType", ""))
            parent_error_msg = truncate_error_msg(message.get("errorMsg", "").strip())

            ns = namespace if (namespace and namespace not in ("", "null")) else ""
            def ns_field(f): return f"{ns}{f}" if ns else f

            org_secret     = get_secret(org_id)
            dest_bucket    = org_secret.get("awsBucket", "cf-ai-common")
            aws_access_key = org_secret.get("accessId")
            aws_secret_key = org_secret.get("accessSecret")
            file_ext       = file_name.rsplit(".", 1)[-1] if "." in file_name else "pdf"

            # ...rest unchanged from here (json fetch, process_child_document, etc.)

            # ── Load JSON body from S3 (or fallback if no json_url) ──
            if json_url:
                bucket, key = parse_s3_url(json_url)
                logger.info(f"[S3-FETCH] bucket={bucket} key={key}")
                obj       = s3.get_object(Bucket=bucket, Key=key)
                json_data = json.load(obj["Body"])
                file_text = json.dumps(json_data)
            else:
                file_text = json.dumps({"error": "Document processing failed"})

            # ── Load child doc ID from DDB (if already pushed to SF before) ──
            ddb_record   = table.get_item(Key={"orgId": org_id, "docId": doc_id}).get("Item", {})
            child_doc_id = (ddb_record.get("childDocId") or "").strip()

            # ── Route to correct handler ──
            if json_url:
                process_child_document(
                    org_id, doc_id, parent_id, message,
                    ns, ns_field, dest_bucket,
                    file_text, file_name, file_ext, doc_sub_type,
                    child_doc_id, parent_error_msg,
                    aws_access_key, aws_secret_key,  
                )
            else:
                process_parent_error(
                    org_id, doc_id, parent_id, message,
                    ns, ns_field, parent_error_msg,
                )

        except Exception as e:
            logger.error(f"[ERROR] {str(e)}")
            logger.error(traceback.format_exc())

            # Safely resolve parent_id even if the crash happened before it was parsed
            parent_id_safe = None
            try:
                parent_id_safe = json.loads(record["body"]).get("parentId")
            except Exception:
                pass

            error_text = truncate_error_msg(f"{str(e)}, docId - {doc_id}")

            # Mark the CHILD (-P-1) as Failed
            try:
                table.update_item(
                    Key                       = {"orgId": org_id, "docId": doc_id},
                    UpdateExpression          = "SET SentToSalesforce=:sf",
                    ExpressionAttributeValues = {":sf": "Failed"},
                )
                logger.info(f"[DDB-FAILED] child={doc_id} SentToSalesforce=Failed")
            except Exception as ddb_err:
                logger.error(f"[DDB-FAILED] Could not update child DynamoDB: {ddb_err}")

            # Write the error message onto the PARENT only
            if parent_id_safe:
                try:
                    table.update_item(
                        Key                       = {"orgId": org_id, "docId": parent_id_safe},
                        UpdateExpression          = "SET errorMsg=:em",
                        ExpressionAttributeValues = {":em": error_text},
                    )
                    logger.info(f"[DDB-FAILED] parent={parent_id_safe} errorMsg='{error_text}'")
                except Exception as ddb_err:
                    logger.error(f"[DDB-FAILED] Could not update parent DynamoDB: {ddb_err}")
            else:
                logger.error(f"[DDB-FAILED] Could not resolve parent_id for docId={doc_id} — errorMsg not written")

            continue

    logger.info("========== SF PUSH LAMBDA END ==========")
    return {"statusCode": 200}
