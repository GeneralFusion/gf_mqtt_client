from pydantic import ValidationError
import pytest
from src.payload_handler import PayloadHandler, Method, ResponseCode

def test_create_general_payload():
    handler = PayloadHandler()
    payload = handler.create_general_payload({"state": "ONLINE"}, "1745534869619")
    assert payload["body"]["state"] == "ONLINE"
    assert payload["timestamp"] == "1745534869619"

    # Test with numeric body
    payload_num = handler.create_general_payload(42, "1745534869619")
    assert payload_num["body"] == 42
    assert payload_num["timestamp"] == "1745534869619"

def test_create_request_payload():
    handler = PayloadHandler()
    payload = handler.create_request_payload(
        Method.GET, "gains", "16fd2706-8baf-433b-82eb-8c7fada847da",
        {"data": [0, 1, 2]}, "123456789==", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    )
    assert payload["header"]["method"] == Method.GET.value
    assert payload["header"]["path"] == "gains"
    assert payload["header"]["request_id"] == "16fd2706-8baf-433b-82eb-8c7fada847da"
    assert payload["header"]["token"] == "123456789=="
    assert payload["header"]["correlation_id"] == "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    assert payload["body"]["data"] == [0, 1, 2]
    assert payload["timestamp"].isdigit()

    # Test without optional fields
    payload_min = handler.create_request_payload(Method.GET, "gains", "16fd2706-8baf-433b-82eb-8c7fada847da")
    assert "token" not in payload_min["header"]
    assert "correlation_id" not in payload_min["header"]
    assert payload_min["body"] is None

def test_create_response_payload():
    handler = PayloadHandler()
    payload = handler.create_response_payload(
        ResponseCode.CONTENT, "gains", "16fd2706-8baf-433b-82eb-8c7fada847da",
        {"data": [0, 1, 2]}, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    )
    assert payload["header"]["response_code"] == ResponseCode.CONTENT.value
    assert payload["header"]["path"] == "gains"
    assert payload["header"]["request_id"] == "16fd2706-8baf-433b-82eb-8c7fada847da"
    assert payload["header"]["correlation_id"] == "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    assert payload["body"]["data"] == [0, 1, 2]
    assert payload["timestamp"].isdigit()

    # Test without correlation_id
    payload_min = handler.create_response_payload(ResponseCode.CONTENT, "gains", "16fd2706-8baf-433b-82eb-8c7fada847da", {"data": [0, 1, 2]})
    assert "correlation_id" not in payload_min["header"]

def test_validate_payload():
    handler = PayloadHandler()

    # Test General Payload
    general_payload = {"body": {"state": "ONLINE"}, "timestamp": "1745534869619"}
    validated_general = handler.validate_payload(general_payload)
    assert validated_general["body"]["state"] == "ONLINE"
    assert validated_general["timestamp"] == "1745534869619"

    # Test Request Payload
    request_payload = {
        "header": {"method": Method.GET.value, "path": "gains", "request_id": "16fd2706-8baf-433b-82eb-8c7fada847da"},
        "body": {"data": [0, 1, 2]},
        "timestamp": "1745534869619"
    }
    validated_request = handler.validate_payload(request_payload)
    assert validated_request["header"]["method"] == Method.GET.value
    assert validated_request["header"]["path"] == "gains"
    assert validated_request["body"]["data"] == [0, 1, 2]

    # Test Response Payload
    response_payload = {
        "header": {"response_code": ResponseCode.CONTENT.value, "path": "gains", "request_id": "16fd2706-8baf-433b-82eb-8c7fada847da"},
        "body": {"data": [0, 1, 2]},
        "timestamp": "1745534869619"
    }
    validated_response = handler.validate_payload(response_payload)
    assert validated_response["header"]["response_code"] == ResponseCode.CONTENT.value
    assert validated_response["header"]["path"] == "gains"
    assert validated_response["body"]["data"] == [0, 1, 2]

    # Test Invalid Payload (wrong method)
    invalid_payload = {
        "header": {"method": 5, "path": "gains", "request_id": "16fd2706-8baf-433b-82eb-8c7fada847da"},
        "timestamp": "1745534869619"
    }
    with pytest.raises(ValueError):
        handler.validate_payload(invalid_payload)

    # Test Invalid JSON (non-dict body)
    invalid_body_payload = {"body": "not_a_dict", "timestamp": "1745534869619"}
    with pytest.raises(ValidationError):
        handler.validate_payload(invalid_body_payload)

def test_parse_payload():
    handler = PayloadHandler()

    # Test valid JSON
    valid_json = '{"body": {"state": "ONLINE"}, "timestamp": "1745534869619"}'
    parsed = handler.parse_payload(valid_json)
    assert parsed["body"]["state"] == "ONLINE"
    assert parsed["timestamp"] == "1745534869619"

    # Test invalid JSON
    invalid_json = "not a json string"
    with pytest.raises(ValueError):
        handler.parse_payload(invalid_json)