import logging
from pydantic_core import ValidationError
import pytest
from gf_mqtt_client import MQTTClient
from tests.axuv.conftest import SUBSYSTEM
from tests.axuv.data_model import DataPayload

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

def test_adjust_data_length(mock_axuv_device, sync_mqtt_requestor, sync_mqtt_responder):
    """Test adjusting the data length of the mock AXUV device."""
    try:
        response = adjust_data_length_via_request(sync_mqtt_requestor, sync_mqtt_responder, 10)
        assert response is not None
        assert "body" in response
        assert mock_axuv_device.data_length == 10

        data = capture_data_via_request(sync_mqtt_requestor, sync_mqtt_responder)
        assert data is not None
        assert all(len(data["metrics"][i]["data"]) == 4*10 for i in range(4)), f"Expected 100 metrics for all channels, got {[len(data['metrics'][i]['data']) for i in range(4)]}"

        response = adjust_data_length_via_request(sync_mqtt_requestor, sync_mqtt_responder, 100)
        assert response is not None
        assert "body" in response
        assert mock_axuv_device.data_length == 100

        data = capture_data_via_request(sync_mqtt_requestor, sync_mqtt_responder)
        assert data is not None
        assert all(len(data["metrics"][i]["data"]) == 4*100 for i in range(4)), f"Expected 100 metrics for all channels, got {[len(data['metrics'][i]['data']) for i in range(4)]}"

    except Exception as e:
        pytest.fail(f"Test failed due to unexpected error: {e}")

def test_arm_device(mock_axuv_device, sync_mqtt_requestor, sync_mqtt_responder):
    """Test the arm functionality of the mock AXUV device."""
    initial_state = mock_axuv_device.status["state"]
    try:
        response = arm_device_via_request(source=sync_mqtt_requestor, target=sync_mqtt_responder)
        assert mock_axuv_device.status["state"] == "ARMED"
        assert initial_state != mock_axuv_device.status["state"]
    except Exception as e:
        pytest.fail(f"Test failed due to unexpected error: {e}")

def test_trigger_device(mock_axuv_device, sync_mqtt_requestor, sync_mqtt_responder):
    """Test the trigger functionality of the mock AXUV device."""
    initial_state = mock_axuv_device.status["state"]
    try:
        arm_device_via_request(source=sync_mqtt_requestor, target=sync_mqtt_responder)
        trigger_device_via_request(source=sync_mqtt_requestor, target=sync_mqtt_responder)
        assert mock_axuv_device.status["state"] == "TRIGGERED"
        assert initial_state != mock_axuv_device.status["state"]
    except Exception as e:
        pytest.fail(f"Test failed due to unexpected error: {e}")


def test_get_data_from_device(mock_axuv_device, sync_mqtt_requestor, sync_mqtt_responder):
    """Test getting data from the mock AXUV device."""
    try:
        arm_device_via_request(source=sync_mqtt_requestor, target=sync_mqtt_responder)
        trigger_device_via_request(source=sync_mqtt_requestor, target=sync_mqtt_responder)
        response = fetch_device_data_via_request(source=sync_mqtt_requestor, target=sync_mqtt_responder)
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


def test_large_data_length(mock_axuv_device, sync_mqtt_requestor, sync_mqtt_responder):
    """Test setting a large data length on the mock AXUV device."""
    try:
        response = adjust_data_length_via_request(sync_mqtt_requestor, sync_mqtt_responder, 32768)
        assert response is not None
        assert "body" in response
        assert mock_axuv_device.data_length == 32768

        data = capture_data_via_request(sync_mqtt_requestor, sync_mqtt_responder)
        assert data is not None
        assert all(len(data["metrics"][i]["data"]) == 4*32768 for i in range(4)), f"Expected 131072 metrics for all channels, got {[len(data['metrics'][i]['data']) for i in range(4)]}"

    except Exception as e:
        pytest.fail(f"Test failed due to unexpected error: {e}")