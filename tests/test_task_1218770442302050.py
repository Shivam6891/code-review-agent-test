import requests

def test_cancel_button_added():
    response = requests.get("https://raw.githubusercontent.com/Shivam6891/code-review-agent-test/main/login.html")
    assert response.status_code == 200
    assert "<button class=\"cancel-btn\">Cancel</button>" in response.text