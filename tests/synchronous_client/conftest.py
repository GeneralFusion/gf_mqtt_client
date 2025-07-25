import pytest

from gf_mqtt_client.message_handler import RequestHandlerBase
from src.gf_mqtt_client.sync_mqtt_client import SyncMQTTClient

from tests.conftest import BROKER_CONFIG, request_handler, generate_uuid

DEVICE_MEMORY_MOCK = {"gains": [0, 1, 2, 3, 4], "status": "ONLINE"}

@pytest.fixture(scope="module")
def requestor():  # mqtt_client is your async one with test responder
    client = SyncMQTTClient(
        broker=BROKER_CONFIG.hostname, port=BROKER_CONFIG.port, timeout=5, identifier=generate_uuid(), username=BROKER_CONFIG.username, password=BROKER_CONFIG.password
    )
    yield client.connect()
    client.disconnect()


@pytest.fixture(scope="module")
def responder():
    client = SyncMQTTClient(
        broker=BROKER_CONFIG.hostname, port=BROKER_CONFIG.port, timeout=3, identifier=generate_uuid(), username=BROKER_CONFIG.username, password=BROKER_CONFIG.password
    )
    client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)
    client.add_message_handler(handler=RequestHandlerBase(process=request_handler, propagate=False))

    yield client.connect()
    client.disconnect()