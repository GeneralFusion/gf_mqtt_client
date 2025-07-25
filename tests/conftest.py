# conftest.py

import logging
import os
import uuid
import pytest
import pytest_asyncio
import time
from typing import Any, Dict

from gf_mqtt_client.models import Method, ResponseCode, MQTTBrokerConfig
from gf_mqtt_client.mqtt_client import MQTTClient, set_compatible_event_loop_policy, reset_event_loop_policy
from gf_mqtt_client.message_handler import ResponseHandlerBase, RequestHandlerBase
from gf_mqtt_client.topic_manager import TopicManager

from dotenv import load_dotenv
load_dotenv()

# Hook into pytest configuration to set the event loop policy
# this is necessary for compatibility with asyncio and aiomqtt with Windows and modern Python versions.
def pytest_configure(config):
    """Configure pytest to use the asyncio event loop."""
    set_compatible_event_loop_policy()


def pytest_unconfigure(config):
    """Reset the event loop policy after tests."""
    reset_event_loop_policy()


BROKER_CONFIG = MQTTBrokerConfig(
    username=os.getenv("MQTT_BROKER_USERNAME"),
    password=os.getenv("MQTT_BROKER_PASSWORD"),
    hostname=os.getenv("MQTT_BROKER_HOSTNAME"),
    port=int(os.getenv("MQTT_BROKER_PORT", 1883))
)

DEVICE_DEFAULTS = {
    "gains": [0, 1, 2, 3, 4],
    "sample_rate": 5000,
    "status": {"last_update": time.time(), "state": "IDLE"},
    "ip": "10.10.10.10",
    "firmware_version": "0.0.1",
}


def generate_uuid() -> str:
    # Generate a unique request ID using UUID
    return str(uuid.uuid4())


class MockMQTTDevice:
    def __init__(self):
        # static resources
        self.gains = DEVICE_DEFAULTS["gains"]
        self.sample_rate = DEVICE_DEFAULTS["sample_rate"]
        self.status = DEVICE_DEFAULTS["status"]
        self.ip = DEVICE_DEFAULTS["ip"]
        self.firmware_version = DEVICE_DEFAULTS["firmware_version"]

        self.uris = ["gains", "sample_rate", "status", "ip", "firmware_version"]
        self.writable_uris = ["gains", "sample_rate"]
        self.action_uris = ["arm", "trigger"]

        # for dynamic POST-created resources
        self.collection_uris = ["resources"]
        self.dynamic_counter = 0
        self.dynamic_resources: Dict[str, Any] = {}

    def update_uri(self, uri: str, value: Any) -> ResponseCode:
        if uri in self.writable_uris:
            setattr(self, uri, value)
            logging.info(f"Updated {uri} to {value}")
            return ResponseCode.CHANGED
        else:
            logging.warning(f"Attempted to update read-only URI: {uri}")
            return ResponseCode.NOT_FOUND

    def get_uri(self, uri: str):
        # check dynamic resources first
        if uri in self.dynamic_resources:
            logging.info(f"Getting dynamic resource: {uri}")
            return ResponseCode.CONTENT, self.dynamic_resources[uri]

        if uri in self.uris:
            logging.info(f"Getting value for URI: {uri}")
            return ResponseCode.CONTENT, getattr(self, uri)
        else:
            logging.warning(f"URI not found: {uri}")
            return ResponseCode.NOT_FOUND, None

    def run_uri(self, uri: str) -> ResponseCode:
        if uri in self.action_uris:
            logging.info(f"Running action for URI: {uri}")
            getattr(self, uri)()
            return ResponseCode.VALID
        else:
            logging.warning(f"Action URI not found: {uri}")
            return ResponseCode.NOT_FOUND

    def arm(self, new_state: bool = True):
        state = "ARMED" if new_state else "IDLE"
        self.update_state(state)
        logging.info(f"Device {'armed' if new_state else 'disarmed'}.")

    def trigger(self):
        if self.status["state"] == "ARMED":
            logging.info("Triggering the device...")
            self.update_state("TRIGGERED")
        else:
            logging.warning("Device is not armed, cannot trigger.")

    def update_state(self, new_state: str):
        self.status = {"last_update": time.time(), "state": new_state}
        logging.info(f"Device state updated to: {new_state}")
        return self.status

    def create_uri(self, collection: str, value: Any) -> (ResponseCode, str):
        """
        Create a new resource under 'collection', return (201_CREATED, new_uri).
        """
        new_id = f"{collection}/{self.dynamic_counter}"
        self.dynamic_counter += 1
        self.dynamic_resources[new_id] = value
        logging.info(f"Created new resource {new_id} = {value}")
        return ResponseCode.CREATED, new_id


MOCK_DEVICE = MockMQTTDevice()


def create_response(request_payload: dict) -> dict:
    header = request_payload["header"]
    path = header["path"]
    method = header["method"]
    body_payload = request_payload.get("body")

    response_code = ResponseCode.NOT_IMPLEMENTED
    response_body = None
    location = None

    if method == Method.GET.value:
        response_code, response_body = MOCK_DEVICE.get_uri(path)

    elif method == Method.PUT.value:
        response_code = MOCK_DEVICE.update_uri(path, body_payload)

    elif method == Method.POST.value:
        # action URIs → 2.04 Changed
        if path in MOCK_DEVICE.action_uris:
            response_code = MOCK_DEVICE.run_uri(path)
        # collection URIs → create new resource
        elif path in MOCK_DEVICE.collection_uris:
            response_code, location = MOCK_DEVICE.create_uri(path, body_payload)
        else:
            response_code = ResponseCode.NOT_FOUND

    # build response header
    resp_header = {
        "response_code": response_code.value,
        "path": path,
        "request_id": header["request_id"],
        "correlation_id": header.get("correlation_id"),
    }
    if location:
        resp_header["location"] = location

    return {
        "header": resp_header,
        "body": response_body,
        "timestamp": str(int(time.time() * 1000)),
    }


async def request_handler(client: MQTTClient, topic: str, payload: dict) -> dict:
    response = create_response(payload)
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
        identifier='responder',
        ensure_unique_identifier=True
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
        identifier='requester',
        ensure_unique_identifier=True
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
