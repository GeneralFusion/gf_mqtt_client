import pytest

from gf_mqtt_client import SyncRequestHandlerBase, SyncMQTTClient, TopicManager
from tests.conftest import BROKER_CONFIG, MOCK_DEVICE

DEVICE_MEMORY_MOCK = {"gains": [0, 1, 2, 3, 4], "status": "ONLINE"}


def sync_request_handler(client: SyncMQTTClient, topic: str, payload: dict) -> dict:
    """
    Synchronous request handler for sync MQTT client tests.
    No async/await - pure blocking I/O.
    """
    response = MOCK_DEVICE.handle_request(payload)
    print(f"[Responder] Responding to {payload['header']['request_id']}")
    response_topic = TopicManager().build_response_topic(request_topic=topic)
    # Use wait=False to avoid blocking the paho network thread
    client.publish(response_topic, response, wait=False)
    return response

@pytest.fixture(scope="function")
def requestor():  # mqtt_client is your async one with test responder
    client = SyncMQTTClient(
        broker=BROKER_CONFIG.hostname,
        port=BROKER_CONFIG.port,
        timeout=BROKER_CONFIG.timeout,
        identifier="requestor",
        username=BROKER_CONFIG.username,
        password=BROKER_CONFIG.password,
        ensure_unique_identifier=True,
    )
    yield client.connect()
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
        handler=SyncRequestHandlerBase(process=sync_request_handler, propagate=False)
    )

    yield client.connect()
    client.disconnect()
