import pytest

from gf_mqtt_client.message_handler import RequestHandlerBase
from src.gf_mqtt_client.sync_mqtt_client import SyncMQTTClient
from src.gf_mqtt_client.mqtt_logger_mixin import setup_mqtt_logging
from tests.conftest import BROKER_CONFIG, request_handler, generate_uuid

DEVICE_MEMORY_MOCK = {"gains": [0, 1, 2, 3, 4], "status": "ONLINE"}

logger = setup_mqtt_logging("gf_mqtt_client", format_style="formatted")

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
        handler=RequestHandlerBase(process=request_handler, propagate=False)
    )

    yield client.connect()
    client.disconnect()
