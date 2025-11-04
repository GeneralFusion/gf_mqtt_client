import pytest

from gf_mqtt_client import (
    MQTTClient,
    SyncMQTTClient, 
    TopicManager
)
from gf_mqtt_client.sync_client import SyncRequestHandlerBase

from tests.integration.axuv.mock_device import MockAXUVDevice
from tests.conftest import BROKER_CONFIG

REQUESTOR_TAG = "axuv_requestor-"
RESPONDER_TAG = "axuv_responder-"
SUBSYSTEM = "axuv"

MOCK_AXUV_DEVICE = MockAXUVDevice()


@pytest.fixture(scope="module")
def mock_axuv_device():
    yield MOCK_AXUV_DEVICE


def request_handler(client: SyncMQTTClient, topic: str, payload: dict) -> dict:
    """Sync request handler for sync client - no async/await."""
    response = MOCK_AXUV_DEVICE.handle_request(payload)
    print(f"[Responder] Responding to {payload['header']['request_id']}")
    response_topic = TopicManager().build_response_topic(request_topic=topic)
    # Use wait=False to avoid blocking the paho network thread
    client.publish(response_topic, response, wait=False)
    return response


@pytest.fixture(scope="function")
def requestor():
    client = SyncMQTTClient(
        broker=BROKER_CONFIG.hostname,
        port=BROKER_CONFIG.port,
        timeout=BROKER_CONFIG.timeout,
        identifier="requestor",
        username=BROKER_CONFIG.username,
        password=BROKER_CONFIG.password,
        ensure_unique_identifier=True,
    )
    client.connect()
    yield client
    client.disconnect()


@pytest.fixture(scope="function")
def responder():
    client = SyncMQTTClient(
        broker=BROKER_CONFIG.hostname,
        port=BROKER_CONFIG.port,
        timeout=BROKER_CONFIG.timeout,
        identifier="responder",
        username=BROKER_CONFIG.username,
        password=BROKER_CONFIG.password,
        ensure_unique_identifier=True,
    )
    client.add_message_handler(
        SyncRequestHandlerBase(process=request_handler, propagate=False)
    )
    client.connect()
    yield client
    client.disconnect()
