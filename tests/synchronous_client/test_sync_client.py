import pytest
from gf_mqtt_client.models import ResponseCode
from gf_mqtt_client.exceptions import BadRequestResponse, InternalServerErrorResponse, NotFoundResponse, UnauthorizedResponse, ResponseException
from tests.conftest import RESPONDER_TAG

TOPIC_SUBSYSTEM = "axuv"
TOPIC_PATH = "gains"


def test_sync_request_success(sync_client, mqtt_responder):
    sync_client.connect()
    mqtt_responder.connect()
    payload = sync_client.request(target_device_tag=RESPONDER_TAG, subsystem=TOPIC_SUBSYSTEM, path=TOPIC_PATH, timeout=1)
    assert payload.get("header").get("response_code") == ResponseCode.CONTENT.value


def test_sync_request_exceptions(sync_client, mqtt_responder):
    sync_client.connect()
    mqtt_responder.connect()

    with pytest.raises(NotFoundResponse):
        sync_client.request(target_device_tag=RESPONDER_TAG, subsystem=TOPIC_SUBSYSTEM, path="nonexisting_path", timeout=100)