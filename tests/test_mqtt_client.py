import time
from typing import Any, Dict
import pytest
import asyncio
from datetime import datetime

from gf_mqtt_client.exceptions import NotFoundResponse, GatewayTimeoutResponse
from gf_mqtt_client.message_handler import ResponseHandlerBase
from gf_mqtt_client.models import Method
from gf_mqtt_client.mqtt_client import MessageHandlerBase
from gf_mqtt_client.payload_handler import ResponseCode
from gf_mqtt_client.exceptions import *

TOPIC_SUBSYSTEM = "axuv"
TOPIC_PATH = "gains"
MAX_REQUESTS = 500  # Maximum concurrent requests for stress tests
DEFAULT_TIMEOUT = 2

# Current date and time for testing
CURRENT_TIMESTAMP = int(datetime(2025, 6, 24, 14, 51).timestamp() * 1000)  # 02:51 PM PDT, June 24, 2025

@pytest.mark.asyncio
async def test_connect(requester, responder):

    assert requester._connected.is_set(), "Client failed to connect"
    assert responder._connected.is_set(), "Responder client failed to connect"


@pytest.mark.asyncio
async def test_request_success(responder, requester):

    response = await requester.request(target_device_tag=responder.identifier, subsystem=TOPIC_SUBSYSTEM, path=TOPIC_PATH)
    assert response is not None, "Expected a valid response, got None"
    assert "header" in response
    assert "response_code" in response["header"]
    assert response["header"]["response_code"] == 205
    assert int(response["timestamp"]) >= CURRENT_TIMESTAMP, "Timestamp should be current or later"


@pytest.mark.asyncio
async def test_request_timeout(responder, requester):

    with pytest.raises(GatewayTimeoutResponse):
        response = await requester.request(
            target_device_tag="nonexistent_device",
            subsystem="nonexistent_subsystem",
            path="nonexistent_path",
            timeout=DEFAULT_TIMEOUT,
        )

@pytest.mark.asyncio
async def test_concurrent_requests(responder, requester):

    async def send(index):
        resp = await requester.request(target_device_tag=responder.identifier, subsystem=TOPIC_SUBSYSTEM, path=TOPIC_PATH)
        assert resp is not None, f"Request {index} got no response"
        assert "header" in resp and "response_code" in resp["header"]
        print(f"[{index}] Response: {resp['header']['response_code']}")

    await asyncio.gather(*[send(i) for i in range(5)])


@pytest.mark.asyncio
async def test_concurrent_requests_stress(responder, requester):
    async def send(index):
        resp = await requester.request(target_device_tag=responder.identifier, subsystem=TOPIC_SUBSYSTEM, path=TOPIC_PATH)
        assert resp is not None, f"Request {index} got no response"
        assert "header" in resp and "response_code" in resp["header"]
        print(f"[{index}] Response: {resp['header']['response_code']}")

    await asyncio.gather(*[send(i) for i in range(MAX_REQUESTS)])  # Increase to stress more

# Updated tests for message handler modularity
@pytest.mark.asyncio
async def test_multiple_handlers(responder, requester):

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
    await requester.add_message_handler(MessageHandlerBase(
        can_handle=lambda p: True,
        process=logging_handler,
        propagate=True
    ))
    await requester.add_message_handler(MessageHandlerBase(
        can_handle=lambda p: "response_code" in p.get("header", {}),
        process=response_handler,
        propagate=False
    ))

    response = await requester.request(target_device_tag=responder.identifier, subsystem=TOPIC_SUBSYSTEM, path=TOPIC_PATH)
    assert response is not None
    assert "header" in response and "response_code" in response["header"]
    # Verify logging occurs before response handling (checked via print order due to addition order)


@pytest.mark.asyncio
async def test_default_handler(responder, requester):

    # Add a handler that doesn't match (e.g., for a different method)
    async def invalid_handler(payload: Dict[str, Any]) -> Dict[str, Any]:
        return None

    await requester.add_message_handler(MessageHandlerBase(
        can_handle=lambda p: "POST" in p.get("header", {}).get("method", ""),
        process=invalid_handler,
        propagate=True
    ))

    # Send a GET request, which should trigger the default handler
    response = await requester.request(target_device_tag=responder.identifier, subsystem=TOPIC_SUBSYSTEM, path=TOPIC_PATH)
    assert response is not None
    assert "header" in response and "response_code" in response["header"]
    # Verify default handler output (checked via print in default_handler)


@pytest.mark.asyncio
async def test_invalid_handler_protocol(requester):

    # Define an invalid handler missing required methods
    class InvalidHandler:
        def handle(self, client):  # Missing topic and payload
            pass

    with pytest.raises(ValueError):
        await requester.add_message_handler(InvalidHandler())


@pytest.mark.asyncio
async def test_put_success(responder, requester):

    response = await requester.request(target_device_tag=responder.identifier, subsystem=TOPIC_SUBSYSTEM, path=TOPIC_PATH, method=Method.PUT, value=0)
    assert response is not None, "Expected a valid response, got None"
    assert "header" in response
    assert "response_code" in response["header"]
    assert response["header"]["response_code"] == ResponseCode.CHANGED.value
    assert int(response["timestamp"]) >= CURRENT_TIMESTAMP, "Timestamp should be current or later"


@pytest.mark.asyncio
async def test_request_bad_request_exception(responder, requester):

    # Use a custom handler that raises exceptions
    from gf_mqtt_client.message_handler import ResponseHandlerBase

    async def exception_raising_handler(client, topic, payload):
        return payload  # it will raise via handle_response_with_exception

    await requester.add_message_handler(ResponseHandlerBase(
        process=exception_raising_handler,
        propagate=False,
        raise_exceptions=True,
    ))

    # Manually inject a simulate_error flag to force NOT_FOUND
    with pytest.raises(NotFoundResponse):
        await requester.request(
            target_device_tag=responder.identifier,
            subsystem=TOPIC_SUBSYSTEM,
            path='bad_request',
        )

@pytest.mark.asyncio
async def test_request_unauthorized_response(responder, requester):

    async def exception_raising_handler(client, topic, payload):
        return payload

    await requester.add_message_handler(ResponseHandlerBase(
        process=exception_raising_handler,
        propagate=False,
        raise_exceptions=True,
    ))

    with pytest.raises(NotFoundResponse):
        await requester.request(
            target_device_tag=responder.identifier,
            subsystem=TOPIC_SUBSYSTEM,
            path="unknown_path",
        )


@pytest.mark.asyncio
async def test_put_then_get_effect(responder, requester):

    # change sample_rate
    new_rate = 12345
    put_resp = await requester.request(
        target_device_tag=responder.identifier,
        subsystem=TOPIC_SUBSYSTEM,
        path="sample_rate",
        method=Method.PUT,
        value=new_rate
    )
    # Assert PUT succeeded
    assert put_resp["header"]["response_code"] == ResponseCode.CHANGED.value

    # Immediately GET the same URI
    get_resp = await requester.request(
        target_device_tag=responder.identifier,
        subsystem=TOPIC_SUBSYSTEM,
        path="sample_rate",
        method=Method.GET
    )
    assert get_resp["header"]["response_code"] == ResponseCode.CONTENT.value
    assert get_resp["body"] == new_rate


import copy

@pytest.mark.asyncio
@pytest.mark.parametrize("uri,new_value", [
    ("gains", [9, 8, 7, 6, 5]),
    ("sample_rate", 25000),
])
async def test_all_writable_uris_put_get(responder, requester, uri, new_value):

    # Issue PUT
    put_resp = await requester.request(
        target_device_tag=responder.identifier,
        subsystem=TOPIC_SUBSYSTEM,
        path=uri,
        method=Method.PUT,
        value=copy.deepcopy(new_value)
    )
    assert put_resp["header"]["response_code"] == ResponseCode.CHANGED.value

    # Then GET and verify
    get_resp = await requester.request(
        target_device_tag=responder.identifier,
        subsystem=TOPIC_SUBSYSTEM,
        path=uri,
        method=Method.GET
    )
    assert get_resp["header"]["response_code"] == ResponseCode.CONTENT.value
    assert get_resp["body"] == new_value


import random

@pytest.mark.asyncio
async def test_concurrent_puts_stress(responder, requester):
    """Fire off MAX_REQUESTS PUTs in parallel, only checking for CHANGED."""
    async def send(i):
        payload = i  # or: [i, i, i, i, i] for gains
        put = await requester.request(
            target_device_tag=responder.identifier,
            subsystem=TOPIC_SUBSYSTEM,
            path="sample_rate",        # or "gains"
            method=Method.PUT,
            value=payload
        )
        assert put["header"]["response_code"] == ResponseCode.CHANGED.value

    await asyncio.gather(*[send(i) for i in range(MAX_REQUESTS)])


@pytest.mark.asyncio
async def test_concurrent_gets_stress(responder, requester):
    """Fire off MAX_REQUESTS GETs in parallel and assert each one succeeds."""

    async def send(i):
        resp = await requester.request(
            target_device_tag=responder.identifier,
            subsystem=TOPIC_SUBSYSTEM,
            path="gains",              # read-only
            method=Method.GET
        )
        # we only assert the status code, not the body
        assert resp["header"]["response_code"] == ResponseCode.CONTENT.value

    await asyncio.gather(*[send(i) for i in range(MAX_REQUESTS)])


# Timestamp used to verify freshness (optional)
CURRENT_TS = int(time.time() * 1000)

@pytest.mark.asyncio
async def test_post_create_and_get_resource(responder, requester):
    """
    POST to the 'resources' collection should return 201 Created with a location,
    and a subsequent GET to that location should return the original body.
    """

    payload = {"foo": "bar", "baz": 123}
    # Issue POST
    post_resp = await requester.request(
        target_device_tag=responder.identifier,
        subsystem="axuv",  # or use TOPIC_SUBSYSTEM if imported
        path="resources",
        method=Method.POST,
        value=payload,
        timeout=DEFAULT_TIMEOUT,
    )
    hdr = post_resp["header"]
    # Assert 201 Created
    assert hdr["response_code"] == ResponseCode.CREATED.value
    # Location header must be present and non-empty
    assert "location" in hdr and isinstance(hdr["location"], str) and hdr["location"]
    new_uri = hdr["location"]

    # Now GET that new resource
    get_resp = await requester.request(
        target_device_tag=responder.identifier,
        subsystem="axuv",
        path=new_uri,
        method=Method.GET
    )
    assert get_resp["header"]["response_code"] == ResponseCode.CONTENT.value
    # Body must match the original payload
    assert get_resp["body"] == payload
    # Fresh timestamp
    assert int(get_resp["timestamp"]) >= CURRENT_TS


@pytest.mark.asyncio
async def test_concurrent_post_create_resources_stress(responder, requester):
    """
    Fire off MAX_REQUESTS concurrent POSTs to 'resources', each creating a unique URI,
    then immediately GET each one and verify its body.
    """
    async def worker(i):
        body = f"stress_val_{i}"
        # POST
        post = await requester.request(
            target_device_tag=responder.identifier,
                subsystem="axuv",
                path="resources",
                method=Method.POST,
                value=body
            )
        assert post["header"]["response_code"] == ResponseCode.CREATED.value
        uri = post["header"]["location"]

        # GET
        get = await requester.request(
            target_device_tag=responder.identifier,
            subsystem="axuv",
            path=uri,
            method=Method.GET
        )
        assert get["header"]["response_code"] == ResponseCode.CONTENT.value
        assert get["body"] == body

    # launch MAX_REQUESTS workers in parallel
    await asyncio.gather(*[worker(i) for i in range(MAX_REQUESTS)])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path,expected_exception",
    [
        ("bad_option", BadOptionResponse),
        ("forbidden", ForbiddenResponse),
        ("not_acceptable", NotAcceptableResponse),
        ("precondition_failed", PreconditionFailedResponse),
        ("request_entity_too_large", RequestEntityTooLargeResponse),
        ("unsupported_content_format", UnsupportedContentFormatResponse),
        ("not_implemented", NotImplementedResponse),
        ("bad_gateway", BadGatewayResponse),
        ("service_unavailable", ServiceUnavailableResponse),
        ("proxying_not_supported", ProxyingNotSupportedResponse),
    ],
)
async def test_coap_error_responses(responder, requester, path, expected_exception):

    async def handler(client, topic, payload):
        return payload

    await requester.add_message_handler(
        ResponseHandlerBase(
            process=handler,
            propagate=False,
            raise_exceptions=True,
        )
    )

    with pytest.raises(expected_exception):
        await requester.request(
            target_device_tag=responder.identifier, subsystem=TOPIC_SUBSYSTEM, path=path
        )
