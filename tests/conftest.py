import json
import pytest
import pytest_asyncio
import asyncio
import time
from src.mqtt_client import MQTTClient  # Your merged class

TEST_TOPIC = "gf_ext_v1/fusion_hall/m3v3/axuv/gain:1:2:a/2D_AD_0_0001"

def create_response(request_payload: dict) -> dict:
    return {
        "header": {
            "response_code": 205,
            "path": request_payload["header"]["path"],
            "request_id": request_payload["header"]["request_id"],
            "correlation_id": request_payload["header"].get("correlation_id"),
        },
        "body": [0, 1, 2, 3, 4],
        "timestamp": str(int(time.time() * 1000))
    }

@pytest_asyncio.fixture(scope="module")
async def mqtt_responder():
    client = MQTTClient(broker="lm26consys.gf.local", port=1893, timeout=5)
    client.username_pw_set("user", "goodlinerCompressi0n!")  # Set your credentials if needed


    await client.connect()
    await client.subscribe("gf_ext_v1/fusion_hall/m3v3/axuv/gain:1:2:a/#")

    async def handler(request_payload: dict) -> dict:
        response = create_response(request_payload)
        print(f"[Responder] Responding to {request_payload['header']['request_id']}")
        return response

    client.set_request_handler(handler)

    yield  # Test runs while responder is active

    await client.disconnect()