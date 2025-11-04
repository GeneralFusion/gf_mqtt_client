import logging
import os
import uuid
import pytest_asyncio
from dotenv import load_dotenv
from pydantic_core import ValidationError

from gf_mqtt_client import (
    MQTTClient,
    MQTTBrokerConfig,
    RequestHandlerBase,
    ResponseHandlerBase,
    TopicManager,
    set_compatible_event_loop_policy,
    reset_event_loop_policy,
)
from .integration.mock_device import MockMQTTDevice

# === Load Environment and Configure Logging ===
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")


# === Pytest Hooks for Event Loop Compatibility ===
def pytest_configure(config):
    set_compatible_event_loop_policy()


def pytest_unconfigure(config):
    reset_event_loop_policy()


# === Utility ===
def generate_uuid() -> str:
    return str(uuid.uuid4())


# === MQTT Broker Config ===
try:
    BROKER_CONFIG = MQTTBrokerConfig(
        username=os.getenv("MQTT_BROKER_USERNAME", "user"),
        password=os.getenv("MQTT_BROKER_PASSWORD", "password"),
        hostname=os.getenv("MQTT_BROKER_HOSTNAME", "broker.emqx.io"),
        port=int(os.getenv("MQTT_BROKER_PORT", 1883)),
    )
except ValidationError as e:
    logging.error(f"Invalid MQTT Broker configuration: {e.json()}")
    raise


MOCK_DEVICE = MockMQTTDevice()


async def request_handler(client: MQTTClient, topic: str, payload: dict) -> dict:
    response = MOCK_DEVICE.handle_request(payload)
    print(f"[Responder] Responding to {payload['header']['request_id']}")
    response_topic = TopicManager().build_response_topic(request_topic=topic)
    await client.publish(response_topic, response)
    return response


@pytest_asyncio.fixture(scope="function")
async def responder():
    client = MQTTClient(
        broker=BROKER_CONFIG.hostname,
        port=BROKER_CONFIG.port,
        timeout=10,
        identifier="responder",
        ensure_unique_identifier=True,
    )
    client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)
    await client.add_message_handler(
        RequestHandlerBase(process=request_handler, propagate=False)
    )
    await client.connect()
    yield client
    await client.disconnect()


@pytest_asyncio.fixture(scope="function")
async def requester():
    client = MQTTClient(
        broker=BROKER_CONFIG.hostname,
        port=BROKER_CONFIG.port,
        timeout=10,
        identifier="requester",
        ensure_unique_identifier=True,
    )
    client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

    async def response_handler(client, topic: str, payload: dict) -> dict:
        if "response_code" in payload.get("header", {}):
            print(f"[Requester] Received response: {payload}")
            return payload

    await client.add_message_handler(
        ResponseHandlerBase(process=response_handler, propagate=False)
    )
    await client.connect()
    yield client
    await client.disconnect()
