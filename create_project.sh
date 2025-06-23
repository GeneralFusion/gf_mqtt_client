#!/bin/bash

# create project directory and basic structure
mkdir -p gf_mqtt_protocol/src
mkdir -p gf_mqtt_protocol/tests

# create python files in src with initial code
cat > gf_mqtt_protocol/src/__init__.py << 'EOF'
EOF

cat > gf_mqtt_protocol/src/mqtt_client.py << 'EOF'
import asyncio
import json
import uuid
from typing import Optional, Dict, Any
import paho.mqtt.client as mqtt
from src.payload_handler import Method

class MQTTClient:
    def __init__(self, broker: str, port: int = 1883):
        self.broker = broker
        self.port = port
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.is_connected = False
        self.loop = asyncio.get_event_loop()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.is_connected = True
            print("Connected to MQTT broker")
        else:
            print(f"Connection failed with code {rc}")

    def on_message(self, client, userdata, msg):
        print(f"Received message on topic {msg.topic}: {msg.payload.decode()}")
        asyncio.create_task(self.handle_message(msg.topic, msg.payload.decode()))

    async def handle_message(self, topic: str, payload: str):
        pass

    async def connect(self):
        await self.loop.run_in_executor(None, self.client.connect, self.broker, self.port)
        self.client.loop_start()
        while not self.is_connected:
            await asyncio.sleep(0.1)

    async def disconnect(self):
        await self.loop.run_in_executor(None, self.client.disconnect)
        self.client.loop_stop()
        self.is_connected = False

    async def publish(self, topic: str, payload: Dict[str, Any], qos: int = 0):
        if not self.is_connected:
            raise ConnectionError("Not connected to MQTT broker")
        payload_str = json.dumps(payload)
        await self.loop.run_in_executor(None, self.client.publish, topic, payload_str, qos)

    async def subscribe(self, topic: str, qos: int = 0):
        if not self.is_connected:
            raise ConnectionError("Not connected to MQTT broker")
        await self.loop.run_in_executor(None, self.client.subscribe, topic, qos)

    async def unsubscribe(self, topic: str):
        if not self.is_connected:
            raise ConnectionError("Not connected to MQTT broker")
        await self.loop.run_in_executor(None, self.client.unsubscribe, topic)

    def generate_request_id(self) -> str:
        return str(uuid.uuid4())

if __name__ == "__main__":
    async def main():
        client = MQTTClient("localhost")
        await client.connect()
        await client.subscribe("gf_ext_v1/fusion_hall/m3v3/axuv/gain:1:2:a/#")
        payload = {
            "header": {"method": Method.GET.value, "path": "gains", "request_id": client.generate_request_id()},
            "body": [0, 1, 2, 3, 4],
            "timestamp": "1745534869619"
        }
        await client.publish("gf_ext_v1/fusion_hall/m3v3/axuv/gain:1:2:a/2D_AD_0_0001", payload)
        await asyncio.sleep(5)
        await client.disconnect()

    asyncio.run(main())
EOF

cat > gf_mqtt_protocol/src/payload_handler.py << 'EOF'
from enum import Enum
from typing import Dict, Any, Optional

class Method(Enum):
    GET = 1
    POST = 2
    PUT = 3
    DELETE = 4

class ResponseCode(Enum):
    CREATED = 201
    DELETED = 202
    VALID = 203
    CHANGED = 204
    CONTENT = 205
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    BAD_OPTION = 402
    FORBIDDEN = 403
    NOT_FOUND = 404
    METHOD_NOT_ALLOWED = 405
    NOT_ACCEPTABLE = 406
    PRECONDITION_FAILED = 412
    REQUEST_ENTITY_TOO_LARGE = 413
    UNSUPPORTED_CONTENT_FORMAT = 415
    INTERNAL_SERVER_ERROR = 500
    NOT_IMPLEMENTED = 501
    BAD_GATEWAY = 502
    SERVICE_UNAVAILABLE = 503
    GATEWAY_TIMEOUT = 504
    PROXYING_NOT_SUPPORTED = 505

class PayloadHandler:
    def __init__(self):
        self.methods = Method
        self.response_codes = ResponseCode

    def create_general_payload(self, body: Dict[str, Any], timestamp: str) -> Dict[str, Any]:
        return {"body": body, "timestamp": timestamp}

    def create_request_payload(self, method: Method, path: str, request_id: str,
                              body: Optional[Dict[str, Any]] = None,
                              token: Optional[str] = None,
                              correlation_id: Optional[str] = None) -> Dict[str, Any]:
        header = {"method": method.value, "path": path, "request_id": request_id}
        if token:
            header["token"] = token
        if correlation_id:
            header["correlation_id"] = correlation_id
        payload = {"header": header}
        if body is not None:
            payload["body"] = body
        payload["timestamp"] = str(self._get_current_timestamp())
        return payload

    def create_response_payload(self, response_code: ResponseCode, path: str, request_id: str,
                               body: Dict[str, Any], correlation_id: Optional[str] = None) -> Dict[str, Any]:
        header = {"response_code": response_code.value, "path": path, "request_id": request_id}
        if correlation_id:
            header["correlation_id"] = correlation_id
        return {"header": header, "body": body, "timestamp": str(self._get_current_timestamp())}

    def parse_payload(self, payload: str) -> Dict[str, Any]:
        return eval(payload)

    def _get_current_timestamp(self) -> int:
        from time import time
        return int(time() * 1000)
EOF

cat > gf_mqtt_protocol/src/topic_manager.py << 'EOF'
class TopicManager:
    def __init__(self):
        self.base_topic = "gf_ext_v1"

    def construct_topic(self, area: str, system: str, subsystem: str, variable: str, device_id: str) -> str:
        return f"{self.base_topic}/{area}/{system}/{subsystem}/{variable}/{device_id}"

    def validate_topic(self, topic: str) -> bool:
        parts = topic.split("/")
        return (len(parts) == 6 and parts[0] == self.base_topic and ":" in parts[4])

    def extract_variable(self, topic: str) -> str:
        if self.validate_topic(topic):
            return topic.split("/")[4]
        raise ValueError("Invalid topic format")
EOF

cat > gf_mqtt_protocol/src/protocol_utils.py << 'EOF'
from enum import Enum
from src.payload_handler import Method, ResponseCode

class ProtocolUtils:
    @staticmethod
    def is_valid_method(method: int) -> bool:
        return method in [m.value for m in Method]

    @staticmethod
    def is_valid_response_code(code: int) -> bool:
        return code in [rc.value for rc in ResponseCode]
EOF

# create test files
cat > gf_mqtt_protocol/tests/__init__.py << 'EOF'
EOF

cat > gf_mqtt_protocol/tests/test_mqtt_client.py << 'EOF'
import pytest
from src.mqtt_client import MQTTClient

@pytest.mark.asyncio
async def test_connect():
    client = MQTTClient("localhost")
    await client.connect()
    assert client.is_connected
    await client.disconnect()
EOF

cat > gf_mqtt_protocol/tests/test_payload_handler.py << 'EOF'
import pytest
from src.payload_handler import PayloadHandler, Method, ResponseCode

def test_create_general_payload():
    handler = PayloadHandler()
    payload = handler.create_general_payload({"state": "ONLINE"}, "1745534869619")
    assert payload["body"]["state"] == "ONLINE"
    assert payload["timestamp"] == "1745534869619"

def test_create_request_payload():
    handler = PayloadHandler()
    payload = handler.create_request_payload(Method.GET, "gains", "16fd2706-8baf-433b-82eb-8c7fada847da",
                                           {"data": [0, 1, 2]}, "123456789==", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
    assert payload["header"]["method"] == Method.GET.value
    assert payload["header"]["path"] == "gains"
    assert payload["header"]["request_id"] == "16fd2706-8baf-433b-82eb-8c7fada847da"
    assert payload["header"]["token"] == "123456789=="
    assert payload["header"]["correlation_id"] == "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    assert payload["body"]["data"] == [0, 1, 2]

def test_create_response_payload():
    handler = PayloadHandler()
    payload = handler.create_response_payload(ResponseCode.CONTENT, "gains", "16fd2706-8baf-433b-82eb-8c7fada847da",
                                            {"data": [0, 1, 2]}, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
    assert payload["header"]["response_code"] == ResponseCode.CONTENT.value
    assert payload["header"]["path"] == "gains"
    assert payload["header"]["request_id"] == "16fd2706-8baf-433b-82eb-8c7fada847da"
    assert payload["header"]["correlation_id"] == "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    assert payload["body"]["data"] == [0, 1, 2]
EOF

cat > gf_mqtt_protocol/tests/test_topic_manager.py << 'EOF'
import pytest
from src.topic_manager import TopicManager

def test_construct_topic():
    manager = TopicManager()
    topic = manager.construct_topic("fusion_hall", "m3v3", "axuv", "gain:1:2:a", "2D_AD_0_0001")
    assert topic == "gf_ext_v1/fusion_hall/m3v3/axuv/gain:1:2:a/2D_AD_0_0001"

def test_validate_topic():
    manager = TopicManager()
    assert manager.validate_topic("gf_ext_v1/fusion_hall/m3v3/axuv/gain:1:2:a/2D_AD_0_0001") == True
    assert manager.validate_topic("invalid/topic") == False
EOF

# create other necessary files
touch gf_mqtt_protocol/README.md
touch gf_mqtt_protocol/uv.lock
touch gf_mqtt_protocol/pyproject.toml

# initialize uv project and add basic dependencies
cd gf_mqtt_protocol || exit
uv init
uv add paho-mqtt pytest

# generate uv.lock
uv lock

echo "Project boilerplate created successfully in gf_mqtt_protocol"
