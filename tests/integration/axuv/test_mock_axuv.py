import json
import logging
import time
from pydantic_core import ValidationError
import pytest
from gf_mqtt_client import MQTTClient
from tests.integration.axuv.conftest import SUBSYSTEM
from tests.integration.axuv.data_model import DataPayload

def arm_device_via_request(source: MQTTClient, target: MQTTClient):
    """Helper function to arm the device via a request."""
    response = source.request(
        target_device_tag=target.identifier,
        subsystem=SUBSYSTEM,
        path="arm",
        method="POST",
        value=True
    )
    return response

def trigger_device_via_request(source: MQTTClient, target: MQTTClient):
    """Helper function to trigger the device via a request."""
    response = source.request(
        target_device_tag=target.identifier,
        subsystem=SUBSYSTEM,
        path="trigger",
        method="POST",
        value=True
    )
    return response

def fetch_device_data_via_request(source: MQTTClient, target: MQTTClient):
    """Helper function to fetch data from the device via a request."""
    response = source.request(
        target_device_tag=target.identifier,
        subsystem=SUBSYSTEM,
        path="data",
        method="GET"
    )
    return response


def capture_data_via_request(source: MQTTClient, target: MQTTClient):
    """Helper function to capture data from the device via a request."""
    try:
        arm_device_via_request(source, target)
        trigger_device_via_request(source, target)
        response = fetch_device_data_via_request(source, target)
        if response is None:
            logging.error("No response received from the device.")
            return None
        if "body" not in response:
            logging.error("Response body is missing.")
            return None
        try:
            payload = DataPayload(**response["body"])
            return payload.model_dump()
        except ValidationError as e:
            logging.error(f"Validation failed: {e.json()[:100]}")
            return None
    except Exception as e:
        logging.error(f"Error during data capture: {e}")
        return None

def adjust_data_length_via_request(source: MQTTClient, target: MQTTClient, new_length: int):
    """Helper function to adjust the data length of the mock AXUV device."""
    response = source.request(
        target_device_tag=target.identifier,
        subsystem=SUBSYSTEM,
        path="data_length",
        method="PUT",
        value=new_length
    )
    return response

def test_mock_axuv_device(mock_axuv_device):
    """Test the mock AXUV device fixture."""
    assert mock_axuv_device is not None
    assert hasattr(mock_axuv_device, 'gains')
    assert hasattr(mock_axuv_device, 'status')
    assert isinstance(mock_axuv_device.gains, list)
    assert isinstance(mock_axuv_device.status, dict)
    assert mock_axuv_device.status["state"] == "IDLE"

def test_adjust_data_length(mock_axuv_device, requestor, responder):
    """Test adjusting the data length of the mock AXUV device."""
    try:
        response = adjust_data_length_via_request(requestor, responder, 10)
        assert response is not None
        assert "body" in response
        assert mock_axuv_device.data_length == 10

        data = capture_data_via_request(requestor, responder)
        assert data is not None
        assert all(len(data["metrics"][i]["data"]) == 4*10 for i in range(4)), f"Expected 100 metrics for all channels, got {[len(data['metrics'][i]['data']) for i in range(4)]}"

        response = adjust_data_length_via_request(requestor, responder, 100)
        assert response is not None
        assert "body" in response
        assert mock_axuv_device.data_length == 100

        data = capture_data_via_request(requestor, responder)
        assert data is not None
        assert all(len(data["metrics"][i]["data"]) == 4*100 for i in range(4)), f"Expected 100 metrics for all channels, got {[len(data['metrics'][i]['data']) for i in range(4)]}"

    except Exception as e:
        pytest.fail(f"Test failed due to unexpected error: {e}")

def test_arm_device(mock_axuv_device, requestor, responder):
    """Test the arm functionality of the mock AXUV device."""
    initial_state = mock_axuv_device.status["state"]
    try:
        response = arm_device_via_request(source=requestor, target=responder)
        assert mock_axuv_device.status["state"] == "ARMED"
        assert initial_state != mock_axuv_device.status["state"]
    except Exception as e:
        pytest.fail(f"Test failed due to unexpected error: {e}")

def test_trigger_device(mock_axuv_device, requestor, responder):
    """Test the trigger functionality of the mock AXUV device."""
    initial_state = mock_axuv_device.status["state"]
    try:
        arm_device_via_request(source=requestor, target=responder)
        trigger_device_via_request(source=requestor, target=responder)
        assert mock_axuv_device.status["state"] == "TRIGGERED"
        assert initial_state != mock_axuv_device.status["state"]
    except Exception as e:
        pytest.fail(f"Test failed due to unexpected error: {e}")


def test_get_data_from_device(mock_axuv_device, requestor, responder):
    """Test getting data from the mock AXUV device."""
    try:
        arm_device_via_request(source=requestor, target=responder)
        trigger_device_via_request(source=requestor, target=responder)
        response = fetch_device_data_via_request(source=requestor, target=responder)
        assert response is not None
        assert "body" in response
    
        try:
            payload = DataPayload(**response["body"])
            payload_str = payload.model_dump_json()[:100] + "..." if len(payload.model_dump_json()) > 100 else payload.model_dump_json()
            logging.info(f"Valid payload: {payload_str}")
        except ValidationError as e:
            pytest.fail(f"Validation failed: {e.json()[:100]}")

    except Exception as e:
        pytest.fail(f"Test failed due to unexpected error: {e}")


@pytest.mark.parametrize("data_length", [1024, 10 * 1024, 32 * 1024, 48 * 1024, 64 * 1024, 80 * 1024, 96 * 1024])
def test_large_data_length(mock_axuv_device, requestor, responder, data_length):
    """Test setting a large data length on the mock AXUV device."""
    try:
        start_time = time.time()
        response = adjust_data_length_via_request(requestor, responder, data_length)
        assert response is not None
        assert "body" in response
        assert mock_axuv_device.data_length == data_length

        data = capture_data_via_request(requestor, responder)
        assert data is not None
        # measure byte size of payload
        payload_str = json.dumps(data)
        payload_size = len(payload_str.encode('utf-8'))
        logging.info(f"Payload size for data length {data_length}: {payload_size} bytes")
        assert len(data["metrics"]) == 4, "Expected 4 metrics for all channels"

        assert all(len(data["metrics"][i]["data"]) == 4*data_length for i in range(4)), f"Expected {4*data_length} metrics for all channels, got {[len(data['metrics'][i]['data']) for i in range(4)]}"
        elapsed_time = time.time() - start_time
        logging.info(f"Data capture for length {data_length} took {elapsed_time:.2f} seconds")
    except Exception as e:
        pytest.fail(f"Test failed due to unexpected error: {e}")