from typing import Any, Dict
import pytest
import asyncio
from gf_mqtt_client.mqtt_client import MQTTClient, MessageHandlerBase
from gf_mqtt_client.payload_handler import ResponseCode, PayloadHandler
from .conftest import RESPONDER_TAG, REQUESTOR_TAG
TOPIC_SUBSYSTEM = "axuv"
TOPIC_PATH = "gains"
from datetime import datetime

# Current date and time for testing
CURRENT_TIMESTAMP = int(datetime(2025, 6, 24, 14, 51).timestamp() * 1000)  # 02:51 PM PDT, June 24, 2025

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
        assert int(response["timestamp"]) >= CURRENT_TIMESTAMP, "Timestamp should be current or later"
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

# Updated tests for message handler modularity
@pytest.mark.asyncio
async def test_multiple_handlers(mqtt_responder, mqtt_requester):
    await mqtt_responder.connect()
    await mqtt_requester.connect()
    try:
        # Add logging handler (propagates)
        async def logging_handler(payload: Dict[str, Any]) -> Dict[str, Any]:
            print(f"Logging: {payload}")
            return None

        # Add response handler (stops propagation)
        async def response_handler(payload: Dict[str, Any]) -> Dict[str, Any]:
            if "response_code" in payload.get("header", {}):
                print(f"Response handled: {payload}")
            return payload

        # Add handlers in a specific order
        await mqtt_requester.add_message_handler(MessageHandlerBase(
            can_handle=lambda p: True,
            process=logging_handler,
            propagate=True
        ))
        await mqtt_requester.add_message_handler(MessageHandlerBase(
            can_handle=lambda p: "response_code" in p.get("header", {}),
            process=response_handler,
            propagate=False
        ))

        response = await mqtt_requester.request(target_device_tag=RESPONDER_TAG, subsystem=TOPIC_SUBSYSTEM, path=TOPIC_PATH)
        assert response is not None
        assert "header" in response and "response_code" in response["header"]
        # Verify logging occurs before response handling (checked via print order due to addition order)
    finally:
        await mqtt_requester.disconnect()
        await mqtt_responder.disconnect()

@pytest.mark.asyncio
async def test_default_handler(mqtt_responder, mqtt_requester):
    await mqtt_responder.connect()
    await mqtt_requester.connect()
    try:
        # Add a handler that doesn't match (e.g., for a different method)
        async def invalid_handler(payload: Dict[str, Any]) -> Dict[str, Any]:
            return None

        await mqtt_requester.add_message_handler(MessageHandlerBase(
            can_handle=lambda p: "POST" in p.get("header", {}).get("method", ""),
            process=invalid_handler,
            propagate=True
        ))

        # Send a GET request, which should trigger the default handler
        response = await mqtt_requester.request(target_device_tag=RESPONDER_TAG, subsystem=TOPIC_SUBSYSTEM, path=TOPIC_PATH)
        assert response is not None
        assert "header" in response and "response_code" in response["header"]
        # Verify default handler output (checked via print in default_handler)
    finally:
        await mqtt_requester.disconnect()
        await mqtt_responder.disconnect()


@pytest.mark.asyncio
async def test_invalid_handler_protocol(mqtt_requester):
    await mqtt_requester.connect()
    try:
        # Define an invalid handler missing required methods
        class InvalidHandler:
            def handle(self, client):  # Missing topic and payload
                pass

        with pytest.raises(ValueError):
            await mqtt_requester.add_message_handler(InvalidHandler())
    finally:
        await mqtt_requester.disconnect()
