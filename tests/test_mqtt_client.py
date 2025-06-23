import pytest
import asyncio
from src.mqtt_client import MQTTClient
from .conftest import RESPONDER_TAG, REQUESTOR_TAG
TOPIC_SUBSYSTEM = "axuv"
TOPIC_PATH = "gains"

@pytest.mark.asyncio
async def test_connect(mqtt_requester):
    await mqtt_requester.connect()
    try:
        assert mqtt_requester._connected.is_set(), "Client failed to connect"
    finally:
        await mqtt_requester.disconnect()

@pytest.mark.asyncio
async def test_request_success(mqtt_responder, mqtt_requester):
    await mqtt_responder.connect()

    await mqtt_requester.connect()

    try:
        response = await mqtt_requester.request(target_device_tag=RESPONDER_TAG, subsystem=TOPIC_SUBSYSTEM, path=TOPIC_PATH)
        assert response is not None, "Expected a valid response, got None"
        assert "header" in response
        assert "response_code" in response["header"]
        assert response["header"]["response_code"] == 205
    finally:
        await mqtt_requester.disconnect()
        await mqtt_responder.disconnect()

@pytest.mark.asyncio
async def test_request_timeout(mqtt_responder, mqtt_requester):
    await mqtt_responder.connect()
    await mqtt_requester.connect()
    try:
        response = await mqtt_requester.request(
            target_device_tag="nonexistent_device",
            subsystem="nonexistent_subsystem",
            path="nonexistent_path"
        )
        assert response is None, "Expected no response due to timeout"
    finally:
        await mqtt_requester.disconnect()
        await mqtt_responder.disconnect()

@pytest.mark.asyncio
async def test_concurrent_requests(mqtt_responder, mqtt_requester):
    await mqtt_responder.connect()
    await mqtt_requester.connect()
    try:
        async def send(index):
            resp = await mqtt_requester.request(target_device_tag=RESPONDER_TAG, subsystem=TOPIC_SUBSYSTEM, path=TOPIC_PATH)
            assert resp is not None, f"Request {index} got no response"
            assert "header" in resp and "response_code" in resp["header"]
            print(f"[{index}] Response: {resp['header']['response_code']}")

        await asyncio.gather(*[send(i) for i in range(5)])
    finally:
        await mqtt_requester.disconnect()
        await mqtt_responder.disconnect()

@pytest.mark.asyncio
async def test_concurrent_requests_stress(mqtt_responder, mqtt_requester):
    await mqtt_responder.connect()
    await mqtt_requester.connect()
    try:
        async def send(index):
            resp = await mqtt_requester.request(target_device_tag=RESPONDER_TAG, subsystem=TOPIC_SUBSYSTEM, path=TOPIC_PATH)
            assert resp is not None, f"Request {index} got no response"
            assert "header" in resp and "response_code" in resp["header"]
            print(f"[{index}] Response: {resp['header']['response_code']}")

        await asyncio.gather(*[send(i) for i in range(100)])  # Increase to stress more
    finally:
        await mqtt_requester.disconnect()
        await mqtt_responder.disconnect()
