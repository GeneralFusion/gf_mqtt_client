import pytest_asyncio
import time
from gf_mqtt_client.models import Method, ResponseCode, MQTTBrokerConfig
from gf_mqtt_client.mqtt_client import MQTTClient
from gf_mqtt_client.message_handler import ResponseHandlerBase, RequestHandlerBase
from gf_mqtt_client.topic_manager import TopicManager

# PUBLIC_BROKER = "broker.emqx.io"

BROKER_CONFIG = MQTTBrokerConfig(
    username = "user",
    password = "goodlinerCompressi0n!",
    hostname = "lm26consys.gf.local",
    port = 1893
)

RESPONDER_TAG = "2D_XX_0_9999"
REQUESTOR_TAG = "2D_XX_0_9998"


DEVICE_DEFAULTS = {
    "gains": [0, 1, 2, 3, 4],
    "sample_rate": 5000,
    "status": {"last_update": time.time(), "state":"IDLE"},
    "ip": "10.10.10.10",
    "firmware_version": "0.0.1",
}

class MockMQTTDevice:

    def __init__(self):
        self.gains = DEVICE_DEFAULTS["gains"]
        self.sample_rate = DEVICE_DEFAULTS["sample_rate"]
        self.status = DEVICE_DEFAULTS["status"]
        self.ip = DEVICE_DEFAULTS["ip"]
        self.firmware_version = DEVICE_DEFAULTS["firmware_version"]

        self.uris = [
            "gains",
            "sample_rate",
            "status",
            "ip",
            "firmware_version"
        ]
        self.writable_uris = [
            "gains",
            "sample_rate"
        ]
        self.action_uris = [
            "arm"
        ]
    
    def update_uri(self, uri, value):
        if uri in self.writable_uris:
            self.__setattr__(uri, value)
            return ResponseCode.CHANGED
        else:
            return ResponseCode.NOT_FOUND

    def get_uri(self, uri):
        if uri in self.uris:
            return self.__getattribute__(uri)
        else:
            return None
        
    def run_uri(self, uri):
        if uri in self.action_uris:
            self.__getattribute__(uri)()
            return ResponseCode.VALID
        else:
            return ResponseCode.NOT_FOUND
        
    def arm(self, new_state=True):
        if new_state:
            self.update_state("ARMED")
        else:
            self.update_state("IDLE")

    def update_state(self, new_state: str):
        self.status = {"last_update": time.time(), "state": self.status}
        return self.status
    
MOCK_DEVICE = MockMQTTDevice()


def create_response(request_payload: dict) -> dict:
    path = request_payload["header"]["path"]
    method = request_payload["header"]["method"]
    value = request_payload.get("body")
    
    body = None

    if method == Method.GET.value:
        body = MOCK_DEVICE.get_uri(path)
        if body:
            response_code = ResponseCode.CONTENT
        else:
            response_code = ResponseCode.NOT_FOUND
            
    elif method == Method.PUT.value:
        response_code = MOCK_DEVICE.update_uri(path, value)

    elif method == Method.POST.value:
        response_code = MOCK_DEVICE.run_uri(path)
        
    else:
        response_code = ResponseCode.NOT_IMPLEMENTED

    return {
        "header": {
            "response_code": response_code.value,
            "path": path,
            "request_id": request_payload["header"]["request_id"],
            "correlation_id": request_payload["header"].get("correlation_id"),
        },
        "body": body,
        "timestamp": str(int(time.time() * 1000)),
    }


async def request_handler(client: MQTTClient, topic: str, payload: dict) -> dict:
    response = create_response(payload)
    print(f"[Responder] Responding to {payload['header']['request_id']}")
    response_topic = TopicManager().build_response_topic(
        request_topic=topic
    )
    await client.publish(response_topic, response)
    return response

@pytest_asyncio.fixture(scope="module")
async def mqtt_responder():
    client = MQTTClient(
        broker=BROKER_CONFIG.hostname, port=BROKER_CONFIG.port, timeout=3, identifier=RESPONDER_TAG
    )
    client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)
    # client.subscriptions.append(f"gf_int_v1/+/request/{RESPONDER_TAG}/+")

    # Update to use MessageHandler instead of set_request_handler
    

    await client.add_message_handler(
        RequestHandlerBase(process=request_handler, propagate=False)
    )

    yield client


@pytest_asyncio.fixture(scope="module")
async def mqtt_requester():
    client = MQTTClient(
        broker=BROKER_CONFIG.hostname, port=BROKER_CONFIG.port, timeout=5, identifier=REQUESTOR_TAG
    )
    client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)
    # Add response handler
    async def response_handler(client: MQTTClient, topic: str, payload: dict) -> dict:
        if "response_code" in payload.get("header", {}):
            print(f"[Requester] Received response: {payload}")
        return payload

    await client.add_message_handler(
        ResponseHandlerBase(process=response_handler, propagate=False)
    )

    yield client
