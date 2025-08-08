# mock_device.py

import time
import math
import logging
from typing import Any, Dict, List

from pydantic import BaseModel
from gf_mqtt_client.models import ResponseCode, Method


class MetricProperties(BaseModel):
    source_name: str
    channel_id: int
    ch_gain: int = 4
    ch_range: int = 5
    ch_offset: int = 0
    wave_byte_order: int = 0
    time_zero: int = 0
    time_range: int = 0
    acq_srate: int = 0
    acq_bit_res: int = 0
    acq_total_samples: int = 0
    checksum: int = 0


class Metric(BaseModel):
    name: str
    timestamp: int
    properties: MetricProperties
    data: str


class DataPayload(BaseModel):
    timestamp: int
    metrics: List[Metric]


class MockAXUVDevice:
    identifier = "mock_axuv"

    def __init__(self):
        self.gains = [0, 1, 2, 3, 4]
        self.sample_rate = 5000
        self.data_length = 100
        self.status = {"last_update": time.time(), "state": "IDLE"}
        self.ip = "10.10.10.10"
        self.firmware_version = "0.0.1"
        self.device_name = "AXUVTE99"

        self._last_payload = DataPayload(timestamp=0, metrics=[])
        self.dynamic_resources: Dict[str, Any] = {}
        self.dynamic_counter = 0

        self._writable = {"gains", "sample_rate", "data_length"}
        self._readable = {
            "gains",
            "sample_rate",
            "data_length",
            "status",
            "ip",
            "firmware_version",
            "data",
        }
        self._actions = {"arm", "trigger"}
        self._collections = {"resources"}

    def handle_request(self, request: dict) -> dict:
        header = request["header"]
        path = header["path"]
        method = header["method"]
        request_id = header["request_id"]
        body = request.get("body")
        correlation_id = header.get("correlation_id")

        if method == Method.GET.value:
            code, response_body = self._handle_get(path)
        elif method == Method.PUT.value:
            code = self._handle_put(path, body)
            response_body = None
        elif method == Method.POST.value:
            code, response_body = self._handle_post(path, body)
        else:
            code, response_body = ResponseCode.NOT_IMPLEMENTED, None

        return self._build_response(
            path=path,
            request_id=request_id,
            correlation_id=correlation_id,
            response_code=code,
            body=response_body,
            location=response_body if code == ResponseCode.CREATED else None,
        )

    def _handle_get(self, path: str):
        if path == "data":
            return ResponseCode.CONTENT, self._last_payload.model_dump()

        if path in self.dynamic_resources:
            return ResponseCode.CONTENT, self.dynamic_resources[path]

        if path in self._readable:
            return ResponseCode.CONTENT, getattr(self, path)

        return ResponseCode.NOT_FOUND, None

    def _handle_put(self, path: str, value: Any) -> ResponseCode:
        if path in self._writable or path in self.dynamic_resources:
            setattr(self, path, value)
            logging.info(f"Updated {path} → {value}")
            return ResponseCode.CHANGED
        elif path in self._readable:
            return ResponseCode.FORBIDDEN
        return ResponseCode.NOT_FOUND

    def _handle_post(self, path: str, value: Any):
        if path in self._actions:
            return self._run_action(path), None
        if path in self._collections:
            return self._create_resource(path, value)
        return ResponseCode.NOT_FOUND, None

    def _run_action(self, name: str) -> ResponseCode:
        method = getattr(self, name, None)
        if callable(method):
            return method()
        return ResponseCode.NOT_FOUND

    def _create_resource(self, collection: str, value: Any) -> (ResponseCode, str):
        new_path = f"{collection}/{self.dynamic_counter}"
        self.dynamic_counter += 1
        self.dynamic_resources[new_path] = value
        return ResponseCode.CREATED, new_path

    def _build_response(
        self,
        path: str,
        request_id: str,
        correlation_id: str,
        response_code: ResponseCode,
        body: Any = None,
        location: str = None,
    ) -> dict:
        header = {
            "response_code": response_code.value,
            "path": path,
            "request_id": request_id,
            "correlation_id": correlation_id,
        }
        if location:
            header["location"] = location
        return {
            "header": header,
            "body": body,
            "timestamp": str(int(time.time() * 1000)),
        }

    def arm(self):
        self.status = {"last_update": time.time(), "state": "ARMED"}
        logging.info("Device armed.")
        return ResponseCode.VALID

    def trigger(self):
        if self.status["state"] != "ARMED":
            logging.warning("Device not armed. Trigger rejected.")
            return ResponseCode.METHOD_NOT_ALLOWED
        
        self.status = {"last_update": time.time(), "state": "TRIGGERED"}

        top_ts = int(time.time())
        metrics = []
        for ch in range(4):
            props = MetricProperties(
                source_name=f"{self.device_name}_chan_{ch + 1}", channel_id=ch
            )
            data = self._generate_hex_data()
            metrics.append(
                Metric(
                    name=self.device_name, timestamp=top_ts, properties=props, data=data
                )
            )

        self._last_payload = DataPayload(timestamp=top_ts, metrics=metrics)
        logging.info(f"Generated payload with 4 channels @ {top_ts}")
        return ResponseCode.VALID

    def _generate_hex_data(self) -> str:
        max_val = 0xFFFF
        N = self.data_length
        return "".join(
            f"{int((math.sin(2 * math.pi * i / N) + 1) * (max_val / 2)):04X}"
            for i in range(N)
        )
