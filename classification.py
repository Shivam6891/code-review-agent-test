import sys
sys.path.insert(0, "/mnt/efs/venv/lib/python3.12/site-packages")

import re
import os
import time
import json
import base64
import pickle
import fitz
from urllib.parse import urlparse, unquote
from botocore.exceptions import ClientError
import boto3
from urllib.parse import quote
from PIL import Image as PILImage
sqs = boto3.client("sqs", region_name="us-east-1")
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/973811007952/sf-push-queue"

# =====================================================
# IMPORT FROM COMMON_METHODS LAMBDA LAYER
# =====================================================
from common_methods import (
    # AWS clients (shared, initialised once)
    s3,
    table,
    bedrock,
    TMP_DIR,

    # S3 utilities
    generate_signed_url,
    download_parent_pdf,

    # PDF utilities
    extract_first_page_text,
    pdf_first_page_to_image,

    # DynamoDB — parent helpers
    claim_parent_for_processing,
    update_parent,
    finalize_parent_based_on_children,

    # DynamoDB — child helpers
    # create_child,
)


# =====================================================
# CLASSIFIER CONFIG  (classification-specific)
# =====================================================

CLASSIFIER_DIR = "/mnt/efs/my_new_model/classification_model"


def find_model(prefix):
    for f in os.listdir(CLASSIFIER_DIR):
        if f.startswith(prefix) and f.endswith(".pkl"):
            return os.path.join(CLASSIFIER_DIR, f)
    raise RuntimeError(f"Missing model: {prefix}")


VECTOR_PATH = find_model("vectorizer")
LSA_PATH    = find_model("lsa")
CLF_PATH    = find_model("classifier")

with open(VECTOR_PATH, "rb") as f:
    vectorizer = pickle.load(f)

with open(LSA_PATH, "rb") as f:
    lsa = pickle.load(f)

with open(CLF_PATH, "rb") as f:
    clf = pickle.load(f)

print("[CLASSIFICATION] Models loaded")


# =====================================================
# LLM PROMPTS  (classification-specific)
# =====================================================

DOC_TYPE_PROMPT = r"""
You are analyzing a PDF document. This document has exactly {ACTUAL_PAGE_COUNT} page(s).

Please classify the document and return a JSON structure for each page with its 
document type, document number, page number, and invoice_subtype (if applicable).

CRITICAL: Only classify pages that actually exist in this {ACTUAL_PAGE_COUNT}-page document.
If this is a 1-page document, return ONLY page_number: 1. Do NOT invent additional pages.

Classify each page as one of: Invoice, Statement, RFQ, Estimate, Pickticket, Creditmemo, Other.


Rules:
- Only classify as RFQ if the document explicitly contains the words RFQ, Quote, Quotation, or Request for Quotation.
- If none of the document types are clearly indicated, classify as Other.
-If the document contains phrases like 'Return', 'Return from', or 'Customer Group Price Return', classify as Creditmemo and use the return reference number as the document number.
- If Pickticket has a matching order/invoice/PO number, use that as the document number.
- Document number: for Invoice = invoice number or PO number; for Statement = statement date; for RFQ/Estimate = quote number or PO number.

For Invoice pages ONLY, also determine the invoice_subtype:
- "Utility Invoice": bills for utilities such as water, electricity, gas, or telecom. Typically has an account number, service address, amount due, and NO itemized line items with quantities/prices.
- "Regular Invoice": has itemized rows with quantities and unit prices.
- Set invoice_subtype to null for all non-Invoice pages.

Return your response as valid JSON only, no additional text.
"""

PICKTICKET_HEADER_PROMPT = r"""
You are a STRICT Pickticket header extraction engine.

Rules:
- Extract ONLY header information
- Do NOT extract line items or tables
- Do NOT guess
- If a value is not visible, return an empty string
- Output MUST be valid JSON
- Wrap JSON inside <JSON> tags

Extract:
- vendorName
- documentNumber (invoice / quote / credit memo / pickticket number)
- poNumber
- documentDate

Return EXACTLY this format:

<JSON>
{
  "header": {
    "vendorName": "",
    "documentNumber": "",
    "poNumber": "",
    "documentDate": ""
  }
}
</JSON>
"""

def create_child(
    parent:           dict,
    page_no:          int,
    label:            str,
    s3_url:           str,
    status:           str  = "Open",
    errorMsg:         str  = "N/A",
    sentToSalesforce: str  = "Draft",
    docSubtype:       str  = None,        # ADD
):
    """
    INSERT a child record the first time; UPDATE it on reprocess.
    Uses a conditional put so two concurrent Lambdas never collide.
    """
    child_doc_id = f"{parent['docId']}-{page_no}"

    base_item = {
        "orgId":            parent["orgId"],
        "docId":            child_doc_id,
        "parentId":         parent["docId"],
        "pageNumber":       page_no,
        "documentType":     label,
        "status":           status,
        "errorMsg":         errorMsg,
        "s3Url":            s3_url,
        "fileName": f"{parent['fileName']}_page-{page_no}{os.path.splitext(parent['fileName'])[1].lower()}",
        "timestamp":        int(time.time()),
        "processedAt":      int(time.time()),
        "SentToSalesforce": sentToSalesforce,
    }

    # Only write docSubtype if it has a value
    if docSubtype is not None:
        base_item["docSubtype"] = docSubtype

    try:
        table.put_item(
            Item=base_item,
            ConditionExpression="attribute_not_exists(docId)",
        )
        print(f"[CHILD CREATED] {child_doc_id}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise

        print(f"[CHILD REPROCESS] Updating {child_doc_id}")

        # Build update expression dynamically based on whether docSubtype exists
        update_expr = """
            SET parentId = :pid,
                pageNumber = :pg,
                documentType = :dt,
                #s = :st,
                errorMsg = :em,
                s3Url = :u,
                fileName = :fn,
                processedAt = :t,
                SentToSalesforce = :sf
            REMOVE jsonUrl, documentNumber, poNumber, documentDate
        """
        expr_values = {
            ":pid": parent["docId"],
            ":pg":  page_no,
            ":dt":  label,
            ":st":  status,
            ":em":  errorMsg,
            ":u":   s3_url,
            ":fn": f"{parent['fileName']}_page-{page_no}{os.path.splitext(parent['fileName'])[1].lower()}",
            ":t":   int(time.time()),
            ":sf":  sentToSalesforce,
        }

        if docSubtype is not None:
            update_expr = update_expr.replace(
                "SentToSalesforce = :sf",
                "SentToSalesforce = :sf,\n                docSubtype = :ds"
            )
            expr_values[":ds"] = docSubtype

        table.update_item(
            Key={"orgId": parent["orgId"], "docId": child_doc_id},
            UpdateExpression=update_expr,
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues=expr_values,
        )

# =====================================================
# ML CLASSIFICATION LOGIC
# =====================================================

ML_CONFIDENCE_THRESHOLD = 0.95


def predict_document_type(text):
    if not text.strip():
        return "Statement", 0.0

    X_vec = vectorizer.transform([text])
    X_red = lsa.transform(X_vec)

    probs = clf.predict_proba(X_red)[0]
    pred  = clf.predict(X_red)[0]

    label_map  = {0: "Statement", 1: "Invoice", 2: "RFQ"}
    label      = label_map.get(pred, "Statement")
    confidence = float(max(probs))

    sorted_probs   = sorted(probs, reverse=True)
    relative_diff  = (sorted_probs[0] - sorted_probs[1]) * 100

    if relative_diff < 10:
        label = "Pickticket"

    return label, confidence


def post_process_prediction(text, predicted_label, confidence):
    keywords_by_label = {
        "Pickticket": ["pickticket", "pick ticket"],
        "Statement":  ["statement"],
        "Invoice":    ["invoice"],
        "RFQ":        ["estimate", "quote", "quotation"],
    }

    lines    = text.splitlines()
    top_text = " ".join(lines[:40]).lower()

    for label, keywords in keywords_by_label.items():
        if any(keyword in top_text for keyword in keywords):
            return label, 1.0

    return "Other", 1.0


def has_acknowledgement(text: str) -> bool:
    t = text.lower()
    return (
        "acknowledgement"    in t
        or "acknowledgment"  in t
        or "acknowledge receipt"  in t
        or "acknowledges receipt" in t
    )


def classify(text):
    if has_acknowledgement(text):
        print("[CLASSIFY][RULE] Acknowledgement detected → Other")
        return "Other", 1.0

    base_label, conf = predict_document_type(text)
    print(f"[CLASSIFY][ML] label={base_label} conf={conf:.3f}")

    if conf >= ML_CONFIDENCE_THRESHOLD:
        print("[CLASSIFY] High confidence ML → bypass rules")
        return base_label, conf

    print("[CLASSIFY] ML not confident → escalate to LLM")
    return "LLM_REQUIRED", conf


# =====================================================
# LLM — DOCUMENT-LEVEL CLASSIFICATION
# =====================================================

def _map_raw_label(raw_label: str) -> str:
    """Normalise a raw LLM label string to a canonical document type."""
    r = raw_label.lower().strip()

    if r in ("rfq", "quote", "quotation", "estimate"):
        return "RFQ"
    if r == "invoice":
        return "Invoice"
    if r == "statement":
        return "Statement"
    if r in ("pickticket", "pick ticket"):
        return "Pickticket"
    if r in ("creditmemo", "credit memo", "return"): 
        return "Invoice"

    return "Other"


def _parse_llm_classification(data: dict | list) -> dict:
    """
    Convert any of the three JSON shapes the LLM may return into a
    uniform  {page_no: {page_number, document_type, document_number}} dict.
    """
    CREDITMEMO_SUBTYPES = {"creditmemo", "credit memo", "return"} 

    final_labels = {}

    # CASE 1 — plain list
    if isinstance(data, list):
        items = data

    # CASE 2 — {"pages": [...]}
    elif isinstance(data, dict) and "pages" in data:
        items = data["pages"]

    # CASE 3 — {"page_1": {...}, "page_2": {...}}
    elif isinstance(data, dict):
        for key, value in data.items():
            m = re.match(r"page[_]?(\d+)", key.lower())
            if not m:
                continue
            page_no   = int(m.group(1))
            raw_label = str(value.get("document_type", ""))
            doc_num   = value.get("document_number", "")
            invoice_subtype = value.get("invoice_subtype", None)

            # Override subtype for CreditMemo / Return
            if raw_label.lower().strip() in CREDITMEMO_SUBTYPES:
                invoice_subtype = "CreditMemo"

            final_labels[page_no] = {
                "page_number":      page_no,
                "document_type":    _map_raw_label(raw_label),
                "document_number":  doc_num,
                "invoice_subtype":  invoice_subtype,
            }
        return final_labels

    else:
        items = []

    for item in items:
        page_no   = int(item.get("page_number", 0))
        raw_label = str(item.get("document_type", ""))
        doc_num   = item.get("document_number", "")
        invoice_subtype = item.get("invoice_subtype", None)

        # Override subtype for CreditMemo / Return
        if raw_label.lower().strip() in CREDITMEMO_SUBTYPES:
            invoice_subtype = "CreditMemo"

        final_labels[page_no] = {
            "page_number":      page_no,
            "document_type":    _map_raw_label(raw_label),
            "document_number":  doc_num,
            "invoice_subtype":  invoice_subtype,
        }
    return final_labels


def classify_doc_type_with_llm(pdf_path: str) -> dict:
    file_ext = os.path.splitext(pdf_path)[1].lower()
    is_pdf = file_ext == ".pdf"
    
    # Get page count (PDFs only)
    if is_pdf:
        doc = fitz.open(pdf_path)
        actual_page_count = doc.page_count
        doc.close()
        print(f"[CLASSIFICATION] Actual PDF page count: {actual_page_count}")
    else:
        actual_page_count = 1
        print(f"[CLASSIFICATION] Processing image as single page")
    
    prompt = DOC_TYPE_PROMPT.replace("{ACTUAL_PAGE_COUNT}", str(actual_page_count))
    
    with open(pdf_path, "rb") as f:
        file_bytes = f.read()

    # Build content based on file type
    if is_pdf:
        content = [{
            "document": {
                "format": "pdf",
                "name": "document",
                "source": {"bytes": file_bytes},
            }
        }, {"text": prompt}]
    else:
        # JPG, PNG, etc.
        format_type = "jpeg" if file_ext in [".jpg", ".jpeg"] else file_ext[1:]
        content = [{
            "image": {
                "format": format_type,
                "source": {"bytes": file_bytes},
            }
        }, {"text": prompt}]
    messages = [{
        "role": "user",
        "content": content
    }]

    response = bedrock.converse(
        modelId="us.anthropic.claude-sonnet-4-5-20250929-v1:0",
        messages=messages,
        inferenceConfig={"maxTokens": 2000, "temperature": 0},
    )

    result = response["output"]["message"]["content"][0]["text"].strip()
    print("------ CLAUDE RAW RESPONSE ------")
    print(result)
    print("---------------------------------")

    try:
        json_match = re.search(r"\{.*\}|\[.*\]", result, re.DOTALL)
        if not json_match:
            raise ValueError("No JSON found in LLM output")

        data = json.loads(json_match.group(0))
        print("PARSED JSON:", json.dumps(data, indent=2))
        return _parse_llm_classification(data)

    except Exception as e:
        print("LLM parse error:", e)
        return {1: {"page_number": 1, "document_type": "Other", "document_number": "", "invoice_subtype": None}}


# =====================================================
# LLM — PICKTICKET HEADER EXTRACTION
# =====================================================

def extract_pickticket_header_with_llm(image_path: str) -> dict:
    with open(image_path, "rb") as f:
        image_b64 = base64.b64encode(f.read()).decode()
    print(f"[BASE64 LENGTH] {len(image_b64)}")  # add this

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 2048,
        "temperature": 0,
        "messages": [{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type":       "base64",
                        "media_type": "image/jpeg",
                        "data":       image_b64,
                    },
                },
                {"type": "text", "text": PICKTICKET_HEADER_PROMPT},
            ],
        }],
    }

    resp  = bedrock.invoke_model(
        modelId="us.anthropic.claude-sonnet-4-5-20250929-v1:0",
        body=json.dumps(body),
    )
    text  = json.loads(resp["body"].read())["content"][0]["text"]
    match = re.search(r"<JSON>(.*?)</JSON>", text, re.DOTALL)

    if not match:
        raise ValueError("Pickticket LLM response missing <JSON>")

    return json.loads(match.group(1))


# =====================================================
# PDF SPLIT + S3 UPLOAD  (classification-specific)
# =====================================================

def split_pdf(pdf_path: str) -> list[str]:
    doc   = fitz.open(pdf_path)
    pages = []
    for i in range(doc.page_count):
        single = fitz.open()
        single.insert_pdf(doc, from_page=i, to_page=i)
        out = f"{TMP_DIR}/page_{i+1}_{int(time.time())}.pdf"
        single.save(out)
        single.close()
        pages.append(out)
    doc.close()
    return pages



def upload_child_pdf(parent: dict, page_no: int, pdf_path: str) -> str:
    parsed = urlparse(parent["s3Url"])
    bucket = parsed.hostname.split(".")[0]

    full_name  = parent['fileName']
    base_name  = re.sub(r'\.[^.]+$', '', full_name)
    
    # Preserve original extension of the child file being uploaded
    child_ext  = os.path.splitext(pdf_path)[1].lower()  # .jpg / .png / .pdf
    
    # Folder name still uses original parent filename for consistency
    parent_ext = os.path.splitext(full_name)[1].lower()
    folder_base = f"{base_name}{parent_ext}"  

    folder     = f"{folder_base}-{page_no}"
    child_file = f"{base_name}{parent_ext}_page-{page_no}{child_ext}"

    key = f"process/{parent['orgId']}/{parent['docId']}/{folder}/{child_file}"

    # Pick correct content type
    content_type = "application/pdf"
    if child_ext in (".jpg", ".jpeg"):
        content_type = "image/jpeg"
    elif child_ext == ".png":
        content_type = "image/png"
    elif child_ext == ".tiff" or child_ext == ".tif":
        content_type = "image/tiff"

    with open(pdf_path, "rb") as f:
        s3.put_object(Bucket=bucket, Key=key, Body=f, ContentType=content_type)

    return f"https://{bucket}.s3.amazonaws.com/{key}"


def upload_classification_json(parent: dict, classification_dict: dict) -> str:
    parsed    = urlparse(parent["s3Url"])
    bucket    = parsed.hostname.split(".")[0]

    org_id    = parent["orgId"]
    doc_id    = parent["docId"]
    full_name = parent["fileName"]                   

    json_key = f"process/{org_id}/{doc_id}/{full_name}/{full_name}_classification.json"

    s3.put_object(
        Bucket=bucket,
        Key=json_key,
        Body=json.dumps({str(k): v for k, v in classification_dict.items()}, indent=2),
        ContentType="application/json",
    )

    encoded_key = "/".join(quote(p, safe="") for p in json_key.split("/"))
    json_url    = f"https://{bucket}.s3.amazonaws.com/{encoded_key}"
    print(f"[CLASSIFICATION SAVED] {json_url}")
    return json_url


def save_pickticket_json(parent: dict, child_doc_id: str, page_pdf_name: str, json_data: dict) -> str:
    folder    = f"{TMP_DIR}/{parent['orgId']}/{child_doc_id}"
    os.makedirs(folder, exist_ok=True)

    base_name = os.path.basename(page_pdf_name)
    if base_name.lower().endswith(".pdf"):
        base_name = base_name[:-4]

    path = os.path.join(folder, f"{base_name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2)

    return path


def pdf_basename_from_s3_url(s3_url: str) -> str:
    parsed = urlparse(s3_url)
    return os.path.basename(unquote(parsed.path.lstrip("/")))


def upload_pickticket_json(page_s3_url: str, json_path: str) -> str:
    parsed = urlparse(page_s3_url)
    bucket = parsed.hostname.split(".")[0]

    key = unquote(parsed.path.lstrip("/"))
    if parsed.fragment:
        key += "#" + parsed.fragment

    folder   = "/".join(key.split("/")[:-1])
    json_key = f"{folder}/{os.path.basename(json_path)}"

    with open(json_path, "rb") as f:
        s3.put_object(Bucket=bucket, Key=json_key, Body=f, ContentType="application/json")

    return f"https://{bucket}.s3.amazonaws.com/{json_key}"


# =====================================================
# LAMBDA HANDLER
# =====================================================

def lambda_handler(event, context):
    print("[CLASSIFICATION] Lambda started")
    print(json.dumps(event))

    org_id = (event or {}).get("orgId")
    doc_id = (event or {}).get("docId")

    if not org_id or not doc_id:
        print("[CLASSIFICATION] Missing orgId/docId in event. Exiting.")
        return {"statusCode": 200}

    # 1) Claim parent (Draft → Processing)
    if not claim_parent_for_processing(org_id, doc_id):
        return {"statusCode": 200}

    table.update_item(
        Key={"orgId": org_id, "docId": doc_id},
        UpdateExpression="SET errorMsg = :em",
        ExpressionAttributeValues={":em": ""}
    )


    # 2) Load parent item
    parent = table.get_item(Key={"orgId": org_id, "docId": doc_id}).get("Item")
    if not parent:
        print(f"[CLASSIFICATION] Parent not found {org_id}/{doc_id}")
        return {"statusCode": 200}

    try:
        # 3) Download parent PDF
        pdf_path = download_parent_pdf(parent)
        print(f"[PARENT] PDF downloaded → {pdf_path}")

        # 4) LLM classification of the full document
        doc_classifications = classify_doc_type_with_llm(pdf_path)
        print("DOCUMENT LEVEL LLM RESULT:", doc_classifications)

        classification_json_url = upload_classification_json(parent, doc_classifications)

        # 5) Split PDF into individual pages
        file_ext = os.path.splitext(pdf_path)[1].lower()

        if file_ext == ".pdf":
            pages = split_pdf(pdf_path)
        else:
            pages = [pdf_path]   # single image — use as-is, no conversion

        print(f"[PARENT] Total pages: {len(pages)}")

        table.update_item(
            Key={"orgId": org_id, "docId": doc_id},
            UpdateExpression="SET pageCount=:pc",
            ExpressionAttributeValues={":pc": len(pages)}
        )

        # 6) Process each page
        for i, page_pdf in enumerate(pages, start=1):
            print(f"\n[PAGE {i}] Processing started")

            page_text = extract_first_page_text(page_pdf)
            ml_label, conf = classify(page_text)

            page_info   = doc_classifications.get(i, {})
            label       = page_info.get("document_type", "Other")
            doc_number  = page_info.get("document_number", "")
            doc_subtype = page_info.get("invoice_subtype", None)

            print(f"[FINAL DECISION] LLM → {label} (DocNo={doc_number}) (ML was {ml_label}, conf={conf:.3f})")

            # Always upload a real child PDF regardless of original format
            page_url     = upload_child_pdf(parent, i, page_pdf)
            child_doc_id = f"{parent['docId']}-{i}"

            # Defaults
            child_status = "Open"
            child_error  = "N/A"
            sent_to_sf   = None
            final_json   = None
            json_url     = None

            # Pickticket handling
            if label == "Pickticket":
                child_status = "Processing"
                sent_to_sf   = "Draft"

                img_path    = pdf_first_page_to_image(page_pdf)
                header_json = extract_pickticket_header_with_llm(img_path)
                os.remove(img_path)

                final_json = {
                    "header": {
                        "vendorName":     header_json.get("header", {}).get("vendorName", ""),
                        "documentNumber": header_json.get("header", {}).get("documentNumber", ""),
                        "poNumber":       header_json.get("header", {}).get("poNumber", ""),
                        "documentDate":   header_json.get("header", {}).get("documentDate", ""),
                    }
                }

                json_path = save_pickticket_json(
                    parent,
                    child_doc_id,
                    pdf_basename_from_s3_url(page_url),
                    final_json,
                )
                json_url = upload_pickticket_json(page_url, json_path)

            elif label == "Other":
                child_status = "Failed"
                child_error  = "Unsupported document type"
                sent_to_sf   = "Draft"

            # Create/update child record
            try:
                create_child(
                    parent=parent,
                    page_no=i,
                    label=label,
                    s3_url=page_url,
                    status=child_status,
                    errorMsg=child_error,
                    sentToSalesforce=sent_to_sf,
                    docSubtype=doc_subtype,    
                )
            except ClientError as ce:
                if ce.response["Error"]["Code"] != "ConditionalCheckFailedException":
                    raise
            # Pickticket enrichment — update DynamoDB with extracted header fields
            if label == "Pickticket":
                table.update_item(
                    Key={"orgId": parent["orgId"], "docId": child_doc_id},
                    UpdateExpression="""
                        SET jsonUrl = :j,
                            documentNumber = :dn,
                            poNumber = :pn,
                            processedAt = :t,
                            #s = :st,
                            SentToSalesforce = :sf
                    """,
                    ExpressionAttributeNames={"#s": "status"},
                    ExpressionAttributeValues={
                        ":j":  json_url,
                        ":dn": final_json["header"]["documentNumber"],
                        ":pn": final_json["header"]["poNumber"],
                        ":t":  int(time.time()),
                        ":st": "Processing",
                        ":sf": "Draft",
                    },
                )

            print(f"[PAGE {i}] FINAL → label={label}")
            if page_pdf != pdf_path:
                os.remove(page_pdf)

        if os.path.exists(pdf_path):
            os.remove(pdf_path)

        # Roll up child failures to parent
        finalize_parent_based_on_children(parent)

        # ── If parent ended up Failed, push to SF ──
        parent_updated = table.get_item(
            Key={"orgId": org_id, "docId": doc_id}
        ).get("Item", {})

        if parent_updated.get("status") == "Failed":
            try:
                sqs.send_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MessageBody=json.dumps({
                        "orgId":     org_id,
                        "docId":     doc_id,
                        "parentId":  doc_id,
                        "jsonUrl":   "",
                        "s3Url":     parent.get("s3Url", ""),
                        "docType":   parent_updated.get("documentType", "Other"),
                        "fileName":  parent.get("fileName", ""),
                        "nameSpace": "",
                        "errorMsg":  parent_updated.get("errorMsg", ""),
                        "pageCount": int(parent_updated.get("pageCount", 0)),  # ← add this
                    })
                )
                print(f"[SQS] Failed parent pushed for {doc_id}")
            except Exception as sqs_err:
                print(f"[SQS ERROR] {sqs_err}")

        return {"statusCode": 200}

    except Exception as e:
        msg = str(e)
        print(f"[ERROR] {doc_id} → {msg} → {e}")

        file_ext = os.path.splitext(parent.get("fileName", ""))[1].lower()
        SUPPORTED_EXTENSIONS = {".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp"}
        if file_ext not in SUPPORTED_EXTENSIONS:
            msg = "Unclassified document type"

        update_parent(parent, "Failed", errorMsg=msg)
        # ── Push to SF so errorMsg reaches Salesforce ──
        try:
            sqs.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps({
                    "orgId":     org_id,
                    "docId":     doc_id,
                    "parentId":  doc_id,
                    "jsonUrl":   "",
                    "s3Url":     parent.get("s3Url", ""),
                    "docType":   parent.get("documentType", "Other"),
                    "fileName":  parent.get("fileName", ""),
                    "nameSpace": "",
                    "errorMsg":  msg       
                })
            )
            print(f"[SQS] Error pushed for parent {doc_id}")
        except Exception as sqs_err:
            print(f"[SQS ERROR] {sqs_err}")

        return {"statusCode": 200}
