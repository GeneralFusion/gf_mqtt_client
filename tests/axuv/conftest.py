import time
import pytest
from gf_mqtt_client.message_handler import RequestHandlerBase
from gf_mqtt_client.models import Method, ResponseCode
from gf_mqtt_client.mqtt_client import MQTTClient
from gf_mqtt_client.sync_mqtt_client import SyncMQTTClient
from mock_device import MockAXUVDevice
from gf_mqtt_client.topic_manager import TopicManager
from tests.conftest import BROKER_CONFIG, generate_uuid

REQUESTOR_TAG = "axuv_requestor-"
RESPONDER_TAG = "axuv_responder-"
SUBSYSTEM = "axuv"

MOCK_AXUV_DEVICE = MockAXUVDevice()

@pytest.fixture(scope="module")
def mock_axuv_device():
    """Fixture to provide a mock AXUV device."""
    yield MOCK_AXUV_DEVICE

def create_response(request_payload: dict) -> dict:
    header = request_payload["header"]
    path = header["path"]
    method = header["method"]
    body_payload = request_payload.get("body")

    response_code = ResponseCode.NOT_IMPLEMENTED
    response_body = None
    location = None

    if method == Method.GET.value:
        response_code, response_body = MOCK_AXUV_DEVICE.get_uri(path)

    elif method == Method.PUT.value:
        response_code = MOCK_AXUV_DEVICE.update_uri(path, body_payload)

    elif method == Method.POST.value:
        # action URIs → 2.04 Changed
        if path in MOCK_AXUV_DEVICE.action_uris:
            response_code = MOCK_AXUV_DEVICE.run_uri(path)
        # collection URIs → create new resource
        elif path in MOCK_AXUV_DEVICE.collection_uris:
            response_code, location = MOCK_AXUV_DEVICE.create_uri(path, body_payload)
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


@pytest.fixture(scope="function")
def requestor():  # mqtt_client is your async one with test responder

    yield SyncMQTTClient(
        broker=BROKER_CONFIG.hostname,
        port=BROKER_CONFIG.port,
        timeout=BROKER_CONFIG.timeout,
        identifier='requestor',
        username=BROKER_CONFIG.username,
        password=BROKER_CONFIG.password,
        ensure_unique_identifier=True
    ).connect()


@pytest.fixture(scope="function")
def responder():
    client = SyncMQTTClient(
        broker=BROKER_CONFIG.hostname,
        port=BROKER_CONFIG.port,
        timeout=BROKER_CONFIG.timeout,
        identifier='responder',
        username=BROKER_CONFIG.username,
        password=BROKER_CONFIG.password,
        ensure_unique_identifier=True
    )
    client.add_message_handler(handler=RequestHandlerBase(process=request_handler, propagate=False))

    yield client.connect()
