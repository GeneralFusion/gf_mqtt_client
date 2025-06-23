import json
import pytest
import pytest_asyncio
import asyncio
import time
from src.mqtt_client import MQTTClient


LOCAL_BROKER = "lm26consys.gf.local"
LOCAL_PORT = 1893
PUBLIC_BROKER = "broker.emqx.io"

USERNAME = "user"
PASSWORD = "goodlinerCompressi0n!"

RESPONDER_TAG = "2D_XX_0_9999"
REQUESTOR_TAG = "2D_XX_0_9998"

def create_response(request_payload: dict) -> dict:
    path = request_payload["header"]["path"]
    assert path=="gains"

    return {
        "header": {
            "response_code": 205,
            "path": path,
            "request_id": request_payload["header"]["request_id"],
            "correlation_id": request_payload["header"].get("correlation_id"),
        },
        "body": [0, 1, 2, 3, 4],
        "timestamp": str(int(time.time() * 1000))
    }

@pytest.fixture(scope="module")
def mqtt_responder():
    client = MQTTClient(broker=LOCAL_BROKER, port=LOCAL_PORT, timeout=3, identifier=RESPONDER_TAG)
    client.set_credentials(USERNAME, PASSWORD)
    client.subscriptions.append(f"gf_int_v1/+/request/{RESPONDER_TAG}/+")

    async def handler(request_payload: dict) -> dict:
        response = create_response(request_payload)
        print(f"[Responder] Responding to {request_payload['header']['request_id']}")
        return response

    client.set_request_handler(handler)
    yield client



@pytest.fixture(scope="module")
def mqtt_requester():
    client = MQTTClient(broker=LOCAL_BROKER, port=LOCAL_PORT, timeout=5, identifier=REQUESTOR_TAG)
    client.set_credentials(USERNAME, PASSWORD)
    client.subscriptions.append("gf_int_v1/+/response/+/+")
    yield client