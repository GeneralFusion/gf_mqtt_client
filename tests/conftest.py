import pytest_asyncio
import time
from src.mqtt_client import MQTTClient
from src.message_handler import ResponseHandlerBase, RequestHandlerBase
from src.topic_manager import TopicManager

LOCAL_BROKER = "lm26consys.gf.local"
LOCAL_PORT = 1893
PUBLIC_BROKER = "broker.emqx.io"

USERNAME = "user"
PASSWORD = "goodlinerCompressi0n!"

RESPONDER_TAG = "2D_XX_0_9999"
REQUESTOR_TAG = "2D_XX_0_9998"


def create_response(request_payload: dict) -> dict:
    path = request_payload["header"]["path"]
    assert path == "gains"

    return {
        "header": {
            "response_code": 205,
            "path": path,
            "request_id": request_payload["header"]["request_id"],
            "correlation_id": request_payload["header"].get("correlation_id"),
        },
        "body": [0, 1, 2, 3, 4],
        "timestamp": str(int(time.time() * 1000)),
    }


@pytest_asyncio.fixture(scope="module")
async def mqtt_responder():
    client = MQTTClient(
        broker=LOCAL_BROKER, port=LOCAL_PORT, timeout=3, identifier=RESPONDER_TAG
    )
    client.set_credentials(USERNAME, PASSWORD)
    client.subscriptions.append(f"gf_int_v1/+/request/{RESPONDER_TAG}/+")

    # Update to use MessageHandler instead of set_request_handler
    async def request_handler(client: MQTTClient, topic: str, payload: dict) -> dict:
        response = create_response(payload)
        print(f"[Responder] Responding to {payload['header']['request_id']}")
        response_topic = TopicManager().build_response_topic(
            request_topic=topic
        )
        await client.publish(response_topic, response)
        return response

    await client.add_message_handler(
        RequestHandlerBase(process=request_handler, propagate=False)
    )

    yield client


@pytest_asyncio.fixture(scope="module")
async def mqtt_requester():
    client = MQTTClient(
        broker=LOCAL_BROKER, port=LOCAL_PORT, timeout=5, identifier=REQUESTOR_TAG
    )
    client.set_credentials(USERNAME, PASSWORD)
    client.subscriptions.append("gf_int_v1/+/response/+/+")

    # Add response handler
    async def response_handler(client: MQTTClient, topic: str, payload: dict) -> dict:
        if "response_code" in payload.get("header", {}):
            print(f"[Requester] Received response: {payload}")
        return payload

    await client.add_message_handler(
        ResponseHandlerBase(process=response_handler, propagate=False)
    )

    yield client
