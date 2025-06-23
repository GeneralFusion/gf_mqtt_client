import pytest
from src.payload_handler import PayloadHandler, Method, ResponseCode

def test_create_general_payload():
    handler = PayloadHandler()
    payload = handler.create_general_payload({"state": "ONLINE"}, "1745534869619")
    assert payload["body"]["state"] == "ONLINE"
    assert payload["timestamp"] == "1745534869619"

def test_create_request_payload():
    handler = PayloadHandler()
    payload = handler.create_request_payload(Method.GET, "gains", "16fd2706-8baf-433b-82eb-8c7fada847da",
                                           {"data": [0, 1, 2]}, "123456789==", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
    assert payload["header"]["method"] == Method.GET.value
    assert payload["header"]["path"] == "gains"
    assert payload["header"]["request_id"] == "16fd2706-8baf-433b-82eb-8c7fada847da"
    assert payload["header"]["token"] == "123456789=="
    assert payload["header"]["correlation_id"] == "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    assert payload["body"]["data"] == [0, 1, 2]

def test_create_response_payload():
    handler = PayloadHandler()
    payload = handler.create_response_payload(ResponseCode.CONTENT, "gains", "16fd2706-8baf-433b-82eb-8c7fada847da",
                                            {"data": [0, 1, 2]}, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
    assert payload["header"]["response_code"] == ResponseCode.CONTENT.value
    assert payload["header"]["path"] == "gains"
    assert payload["header"]["request_id"] == "16fd2706-8baf-433b-82eb-8c7fada847da"
    assert payload["header"]["correlation_id"] == "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    assert payload["body"]["data"] == [0, 1, 2]
