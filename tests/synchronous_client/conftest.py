import pytest

from gf_mqtt_client.message_handler import RequestHandlerBase
from src.gf_mqtt_client.sync_mqtt_client import SyncMQTTClient

from tests.conftest import BROKER_CONFIG, REQUESTOR_TAG, RESPONDER_TAG, request_handler

DEVICE_MEMORY_MOCK = {"gains": [0, 1, 2, 3, 4], "status": "ONLINE"}

@pytest.fixture(scope="module")
def sync_mqtt_requestor():  # mqtt_client is your async one with test responder
    yield SyncMQTTClient(
        broker=BROKER_CONFIG.hostname, port=BROKER_CONFIG.port, timeout=5, identifier=REQUESTOR_TAG, username=BROKER_CONFIG.username, password=BROKER_CONFIG.password
    )


@pytest.fixture(scope="module")
def sync_mqtt_responder():
    client = SyncMQTTClient(
        broker=BROKER_CONFIG.hostname, port=BROKER_CONFIG.port, timeout=3, identifier=RESPONDER_TAG
    )
    client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)
    client.add_message_handler(handler=RequestHandlerBase(process=request_handler, propagate=False))

    yield client