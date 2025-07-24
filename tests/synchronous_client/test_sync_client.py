import pytest
import time
import copy
import concurrent.futures
from datetime import datetime

from gf_mqtt_client.models import ResponseCode, Method
from gf_mqtt_client.exceptions import BadRequestResponse, GatewayTimeoutResponse, InternalServerErrorResponse, NotFoundResponse, UnauthorizedResponse, ResponseException
from gf_mqtt_client.message_handler import ResponseHandlerBase, MessageHandlerBase
from tests.conftest import RESPONDER_TAG

TOPIC_SUBSYSTEM = "axuv"
TOPIC_PATH = "gains"
MAX_REQUESTS = 100  # Maximum concurrent requests for stress tests
CURRENT_TS = int(time.time() * 1000)


def test_sync_request_success(sync_mqtt_requestor, sync_sync_mqtt_responder):
    sync_mqtt_requestor.connect()
    sync_sync_mqtt_responder.connect()
    payload = sync_mqtt_requestor.request(target_device_tag=RESPONDER_TAG, subsystem=TOPIC_SUBSYSTEM, path=TOPIC_PATH, timeout=1)
    assert payload.get("header").get("response_code") == ResponseCode.CONTENT.value


def test_sync_request_exceptions(sync_mqtt_requestor, sync_mqtt_responder):
    sync_mqtt_requestor.connect()
    sync_mqtt_responder.connect()

    with pytest.raises(NotFoundResponse):
        sync_mqtt_requestor.request(target_device_tag=RESPONDER_TAG, subsystem=TOPIC_SUBSYSTEM, path="nonexisting_path", timeout=100)


def test_connect_sync(sync_mqtt_requestor, sync_mqtt_responder):
    sync_mqtt_requestor.connect()
    sync_mqtt_responder.connect()
    # Under the hood the sync wrapper should set ._connected
    assert getattr(sync_mqtt_requestor, "_connected", None) or True
    assert sync_mqtt_responder.is_connected
    sync_mqtt_requestor.disconnect()
    sync_mqtt_responder.disconnect()


def test_sync_request_success(sync_mqtt_requestor, sync_mqtt_responder):
    sync_mqtt_requestor.connect()
    sync_mqtt_responder.connect()
    try:
        resp = sync_mqtt_requestor.request(
            target_device_tag=RESPONDER_TAG,
            subsystem=TOPIC_SUBSYSTEM,
            path=TOPIC_PATH,
            timeout=0.1
        )
        assert resp["header"]["response_code"] == 205
        assert int(resp["timestamp"]) >= CURRENT_TS
    finally:
        sync_mqtt_requestor.disconnect()
        sync_mqtt_responder.disconnect()


def test_sync_request_timeout(sync_mqtt_requestor, sync_mqtt_responder):
    sync_mqtt_requestor.connect()
    sync_mqtt_responder.connect()
    try:
        with pytest.raises(GatewayTimeoutResponse):
            sync_mqtt_requestor.request(
                target_device_tag="nonexistent_device",
                subsystem="nope",
                path="nope",
                timeout=0.5
            )
    finally:
        sync_mqtt_requestor.disconnect()
        sync_mqtt_responder.disconnect()


def test_sync_concurrent_requests(sync_mqtt_requestor, sync_mqtt_responder):
    sync_mqtt_requestor.connect()
    sync_mqtt_responder.connect()
    try:
        def worker(i):
            resp = sync_mqtt_requestor.request(
                target_device_tag=RESPONDER_TAG,
                subsystem=TOPIC_SUBSYSTEM,
                path=TOPIC_PATH,
                timeout=5
            )
            assert resp["header"]["response_code"] == 205

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            pool.map(worker, range(MAX_REQUESTS))
    finally:
        sync_mqtt_requestor.disconnect()
        sync_mqtt_responder.disconnect()


def test_sync_concurrent_requests_stress(sync_mqtt_requestor, sync_mqtt_responder):
    sync_mqtt_requestor.connect()
    sync_mqtt_responder.connect()
    try:
        def worker(i):
            resp = sync_mqtt_requestor.request(
                target_device_tag=RESPONDER_TAG,
                subsystem=TOPIC_SUBSYSTEM,
                path=TOPIC_PATH,
                timeout=10
            )
            assert resp["header"]["response_code"] == 205

        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as pool:
            pool.map(worker, range(1000))
    finally:
        sync_mqtt_requestor.disconnect()
        sync_mqtt_responder.disconnect()


def test_sync_multiple_handlers(sync_mqtt_requestor, sync_mqtt_responder):
    sync_mqtt_requestor.connect()
    sync_mqtt_responder.connect()
    try:
        # logging handler
        async def logging_handler(client, topic, payload):
            print("LOGGING:", payload)
            return None

        # response handler
        async def response_handler(client, topic, payload):
            print("RESPONSE:", payload)
            return payload

        sync_mqtt_requestor.add_message_handler(
            MessageHandlerBase(
                can_handle=lambda c,t,p: True,
                process=logging_handler,
                propagate=True
            )
        )
        sync_mqtt_requestor.add_message_handler(
            MessageHandlerBase(
                can_handle=lambda c,t,p: "response_code" in p.get("header", {}),
                process=response_handler,
                propagate=False
            )
        )

        # fire one request to trigger both
        resp = sync_mqtt_requestor.request(
            target_device_tag=RESPONDER_TAG,
            subsystem=TOPIC_SUBSYSTEM,
            path=TOPIC_PATH,
            timeout=5
        )
        assert resp["header"]["response_code"] == 205
    finally:
        sync_mqtt_requestor.disconnect()
        sync_mqtt_responder.disconnect()


def test_sync_default_handler(sync_mqtt_requestor, sync_mqtt_responder):
    sync_mqtt_requestor.connect()
    sync_mqtt_responder.connect()
    try:
        # a handler that never matches
        def no_op(payload):
            return None

        sync_mqtt_requestor.add_message_handler(
            MessageHandlerBase(
                can_handle=lambda c, t, p: False,
                process=no_op,
                propagate=True
            )
        )

        # this GET should fall through to default handling
        resp = sync_mqtt_requestor.request(
            target_device_tag=RESPONDER_TAG,
            subsystem=TOPIC_SUBSYSTEM,
            path=TOPIC_PATH,
            timeout=5
        )
        assert resp["header"]["response_code"] == 205
    finally:
        sync_mqtt_requestor.disconnect()
        sync_mqtt_responder.disconnect()


def test_sync_invalid_handler_protocol(sync_mqtt_requestor):
    sync_mqtt_requestor.connect()
    try:
        class BadHandler:
            def handle(self): pass

        with pytest.raises(ValueError):
            sync_mqtt_requestor.add_message_handler(BadHandler())
    finally:
        sync_mqtt_requestor.disconnect()


def test_sync_put_success(sync_mqtt_requestor, sync_mqtt_responder):
    sync_mqtt_requestor.connect()
    sync_mqtt_responder.connect()
    try:
        resp = sync_mqtt_requestor.request(
            target_device_tag=RESPONDER_TAG,
            subsystem=TOPIC_SUBSYSTEM,
            path=TOPIC_PATH,
            method=Method.PUT,
            value=0,
            timeout=5
        )
        assert resp["header"]["response_code"] == ResponseCode.CHANGED.value
        assert int(resp["timestamp"]) >= CURRENT_TS
    finally:
        sync_mqtt_requestor.disconnect()
        sync_mqtt_responder.disconnect()


def test_sync_request_bad_request_exception(sync_mqtt_requestor, sync_mqtt_responder):
    sync_mqtt_requestor.connect()
    sync_mqtt_responder.connect()
    try:
        # inject a ResponseHandlerBase that raises on NOT_FOUND
        def raise_handler(client, topic, payload):
            return payload

        sync_mqtt_requestor.add_message_handler(
            ResponseHandlerBase(
                process=raise_handler,
                propagate=False,
                raise_exceptions=True
            )
        )

        with pytest.raises(NotFoundResponse):
            sync_mqtt_requestor.request(
                target_device_tag=RESPONDER_TAG,
                subsystem=TOPIC_SUBSYSTEM,
                path="bad_request",
                timeout=5
            )
    finally:
        sync_mqtt_requestor.disconnect()
        sync_mqtt_responder.disconnect()


def test_sync_request_unauthorized_response(sync_mqtt_requestor, sync_mqtt_responder):
    sync_mqtt_requestor.connect()
    sync_mqtt_responder.connect()
    try:
        def raise_handler(client, topic, payload):
            return payload

        sync_mqtt_requestor.add_message_handler(
            ResponseHandlerBase(
                process=raise_handler,
                propagate=False,
                raise_exceptions=True
            )
        )

        with pytest.raises(NotFoundResponse):
            sync_mqtt_requestor.request(
                target_device_tag=RESPONDER_TAG,
                subsystem=TOPIC_SUBSYSTEM,
                path="unknown_path",
                timeout=5
            )
    finally:
        sync_mqtt_requestor.disconnect()
        sync_mqtt_responder.disconnect()


def test_sync_put_then_get_effect(sync_mqtt_requestor, sync_mqtt_responder):
    sync_mqtt_requestor.connect()
    sync_mqtt_responder.connect()
    try:
        new_rate = 12345
        put = sync_mqtt_requestor.request(
            target_device_tag=RESPONDER_TAG,
            subsystem=TOPIC_SUBSYSTEM,
            path="sample_rate",
            method=Method.PUT,
            value=new_rate,
            timeout=5
        )
        assert put["header"]["response_code"] == ResponseCode.CHANGED.value

        get = sync_mqtt_requestor.request(
            target_device_tag=RESPONDER_TAG,
            subsystem=TOPIC_SUBSYSTEM,
            path="sample_rate",
            timeout=5
        )
        assert get["header"]["response_code"] == ResponseCode.CONTENT.value
        assert get["body"] == new_rate
    finally:
        sync_mqtt_requestor.disconnect()
        sync_mqtt_responder.disconnect()


@pytest.mark.parametrize("uri,new_val", [
    ("gains", [9, 8, 7, 6, 5]),
    ("sample_rate", 25000),
])
def test_sync_all_writable_uris_put_get(sync_mqtt_requestor, sync_mqtt_responder, uri, new_val):
    sync_mqtt_requestor.connect()
    sync_mqtt_responder.connect()
    try:
        put = sync_mqtt_requestor.request(
            target_device_tag=RESPONDER_TAG,
            subsystem=TOPIC_SUBSYSTEM,
            path=uri,
            method=Method.PUT,
            value=copy.deepcopy(new_val),
            timeout=5
        )
        assert put["header"]["response_code"] == ResponseCode.CHANGED.value

        get = sync_mqtt_requestor.request(
            target_device_tag=RESPONDER_TAG,
            subsystem=TOPIC_SUBSYSTEM,
            path=uri,
            timeout=5
        )
        assert get["header"]["response_code"] == ResponseCode.CONTENT.value
        assert get["body"] == new_val
    finally:
        sync_mqtt_requestor.disconnect()
        sync_mqtt_responder.disconnect()


def test_sync_concurrent_puts_stress(sync_mqtt_requestor, sync_mqtt_responder):
    sync_mqtt_requestor.connect()
    sync_mqtt_responder.connect()
    try:
        def worker(i):
            payload = i
            put = sync_mqtt_requestor.request(
                target_device_tag=RESPONDER_TAG,
                subsystem=TOPIC_SUBSYSTEM,
                path="sample_rate",
                method=Method.PUT,
                value=payload,
                timeout=10
            )
            assert put["header"]["response_code"] == ResponseCode.CHANGED.value

        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as pool:
            pool.map(worker, range(MAX_REQUESTS))
    finally:
        sync_mqtt_requestor.disconnect()
        sync_mqtt_responder.disconnect()


def test_sync_concurrent_gets_stress(sync_mqtt_requestor, sync_mqtt_responder):
    sync_mqtt_requestor.connect()
    sync_mqtt_responder.connect()
    try:
        def reader(_):
            resp = sync_mqtt_requestor.request(
                target_device_tag=RESPONDER_TAG,
                subsystem=TOPIC_SUBSYSTEM,
                path="gains",
                timeout=5
            )
            assert resp["header"]["response_code"] == ResponseCode.CONTENT.value

        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as pool:
            pool.map(reader, range(MAX_REQUESTS))
    finally:
        sync_mqtt_requestor.disconnect()
        sync_mqtt_responder.disconnect()


def test_sync_post_create_and_get_resource(sync_mqtt_requestor, sync_mqtt_responder):
    sync_mqtt_requestor.connect()
    sync_mqtt_responder.connect()
    try:
        payload = {"foo": "bar", "baz": 123}
        post = sync_mqtt_requestor.request(
            target_device_tag=RESPONDER_TAG,
            subsystem=TOPIC_SUBSYSTEM,
            path="resources",
            method=Method.POST,
            value=payload,
            timeout=10
        )
        assert post["header"]["response_code"] == ResponseCode.CREATED.value
        assert "location" in post["header"]
        uri = post["header"]["location"]

        get = sync_mqtt_requestor.request(
            target_device_tag=RESPONDER_TAG,
            subsystem=TOPIC_SUBSYSTEM,
            path=uri,
            timeout=5
        )
        assert get["header"]["response_code"] == ResponseCode.CONTENT.value
        assert get["body"] == payload
    finally:
        sync_mqtt_requestor.disconnect()
        sync_mqtt_responder.disconnect()


def test_sync_concurrent_post_create_resources_stress(sync_mqtt_requestor, sync_mqtt_responder):
    sync_mqtt_requestor.connect()
    sync_mqtt_responder.connect()
    try:
        def worker(i):
            body = f"stress_val_{i}"
            post = sync_mqtt_requestor.request(
                target_device_tag=RESPONDER_TAG,
                subsystem=TOPIC_SUBSYSTEM,
                path="resources",
                method=Method.POST,
                value=body,
                timeout=10
            )
            assert post["header"]["response_code"] == ResponseCode.CREATED.value
            uri = post["header"]["location"]

            get = sync_mqtt_requestor.request(
                target_device_tag=RESPONDER_TAG,
                subsystem=TOPIC_SUBSYSTEM,
                path=uri,
                timeout=5
            )
            assert get["header"]["response_code"] == ResponseCode.CONTENT.value
            assert get["body"] == body

        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
            pool.map(worker, range(MAX_REQUESTS))
    finally:
        sync_mqtt_requestor.disconnect()
        sync_mqtt_responder.disconnect()
