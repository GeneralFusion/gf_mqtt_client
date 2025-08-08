# conftest.py

import time
import pytest

from gf_mqtt_client.message_handler import RequestHandlerBase
from gf_mqtt_client.models import Method
from gf_mqtt_client.mqtt_client import MQTTClient
from gf_mqtt_client.sync_mqtt_client import SyncMQTTClient
from gf_mqtt_client.topic_manager import TopicManager

from mock_device import MockAXUVDevice
from tests.conftest import BROKER_CONFIG

REQUESTOR_TAG = "axuv_requestor-"
RESPONDER_TAG = "axuv_responder-"
SUBSYSTEM = "axuv"

MOCK_AXUV_DEVICE = MockAXUVDevice()


@pytest.fixture(scope="module")
def mock_axuv_device():
    yield MOCK_AXUV_DEVICE


async def request_handler(client: MQTTClient, topic: str, payload: dict) -> dict:
    response = MOCK_AXUV_DEVICE.handle_request(payload)
    print(f"[Responder] Responding to {payload['header']['request_id']}")
    response_topic = TopicManager().build_response_topic(request_topic=topic)
    await client.publish(response_topic, response)
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
        RequestHandlerBase(process=request_handler, propagate=False)
    )
    client.connect()
    yield client
    client.disconnect()
