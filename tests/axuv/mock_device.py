from pydantic import BaseModel
from typing import List, Dict, Any
import math
import time
import logging
from src.gf_mqtt_client import ResponseCode

DEVICE_DEFAULTS = {
    "gains": [0, 1, 2, 3, 4],
    "sample_rate": 5000,
    "status": {"last_update": time.time(), "state": "IDLE"},
    "ip": "10.10.10.10",
    "firmware_version": "0.0.1",
}


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
    def __init__(self):
        # static resources
        self.gains = DEVICE_DEFAULTS["gains"]
        self.sample_rate = DEVICE_DEFAULTS["sample_rate"]
        self.status = DEVICE_DEFAULTS["status"]
        self.ip = DEVICE_DEFAULTS["ip"]
        self.firmware_version = DEVICE_DEFAULTS["firmware_version"]

        # how many samples per channel
        self.data_length = DEVICE_DEFAULTS.get("data_length", 100)
        # device identifier used in the JSON
        self.device_name = DEVICE_DEFAULTS.get("device_name", "AXUVTE99")

        # buffer to hold last-triggered payload
        self._last_payload: DataPayload = DataPayload(timestamp=0, metrics=[])

        # URIs
        self.uris = [
            "gains",
            "sample_rate",
            "status",
            "ip",
            "firmware_version",
            "data",
            "data_length",
        ]
        self.writable_uris = ["gains", "sample_rate", "data_length"]
        self.action_uris = ["arm", "trigger"]

        # dynamic‐POST resources
        self.collection_uris = ["resources"]
        self.dynamic_counter = 0
        self.dynamic_resources: Dict[str, Any] = {}

    def update_uri(self, uri: str, value: Any) -> ResponseCode:
        if uri in self.writable_uris or uri in self.dynamic_resources:
            setattr(self, uri, value)
            logging.info(f"Updated {uri} to {value}")
            return ResponseCode.CHANGED
        elif uri in self.uris:
            logging.warning(f"Attempted to update read-only URI: {uri}")
            return ResponseCode.FORBIDDEN
        else:
            logging.warning(f"URI not found for update: {uri}")
            return ResponseCode.NOT_FOUND

    def get_uri(self, uri: str):
        # dynamic resources first
        if uri in self.dynamic_resources:
            return ResponseCode.CONTENT, self.dynamic_resources[uri]

        # return the latest Pydantic payload as dict
        if uri == "data":
            return ResponseCode.CONTENT, self._last_payload.dict()

        if uri in self.uris:
            return ResponseCode.CONTENT, getattr(self, uri)

        logging.warning(f"URI not found: {uri}")
        return ResponseCode.NOT_FOUND, None

    def _generate_hex_data(self) -> str:
        """
        Build a sine wave of length `self.data_length`,
        scale to 0–0xFFFF, convert each to 4‑hex digits,
        concatenate (no delimiters).
        """
        max_val = 0xFFFF
        N = self.data_length
        out = []
        for i in range(N):
            angle = 2 * math.pi * i / N
            val = int((math.sin(angle) + 1) * (max_val / 2))
            out.append(f"{val:04X}")
        return "".join(out)

    def run_uri(self, uri: str, value: Any = None) -> ResponseCode:
        if uri not in self.action_uris:
            logging.warning(f"Action URI not found: {uri}")
            return ResponseCode.NOT_FOUND

        logging.info(f"Running action for URI: {uri}")
        try:
            if value is not None:
                # If the action requires a value, call it with the value
                return getattr(self, uri)(value)
            else:
                # If no value is needed, just call the action
                return getattr(self, uri)()
        except Exception as e:
            logging.error(f"Error running action {uri}: {e}")
            return ResponseCode.INTERNAL_ERROR

    def arm(self, new_state: bool = True):
        state = "ARMED" if new_state else "IDLE"
        self.update_state(state)
        logging.info(f"Device {'armed' if new_state else 'disarmed'}.")
        return ResponseCode.VALID

    def trigger(self):
        if self.status["state"] == "ARMED":
            logging.info("Triggering the device...")
            self.update_state("TRIGGERED")

            top_ts = int(time.time())
            metrics = []
            for ch in range(4):
                props = MetricProperties(
                    source_name=f"{self.device_name}_chan_{ch+1}", channel_id=ch
                )
                data_hex = self._generate_hex_data()
                metrics.append(
                    Metric(
                        name=self.device_name,
                        timestamp=top_ts,
                        properties=props,
                        data=data_hex,
                    )
                )

            # create Pydantic payload
            self._last_payload = DataPayload(timestamp=top_ts, metrics=metrics)
            logging.info(f"Generated 4‑channel Pydantic payload @ {top_ts}")
        else:
            logging.warning("Device is not armed, cannot trigger.")
            return ResponseCode.METHOD_NOT_ALLOWED
        return ResponseCode.VALID

    def update_state(self, new_state: str):
        self.status = {"last_update": time.time(), "state": new_state}
        logging.info(f"Device state updated to: {new_state}")
        return self.status

    def create_uri(self, collection: str, value: Any) -> (ResponseCode, str):
        new_id = f"{collection}/{self.dynamic_counter}"
        self.dynamic_counter += 1
        self.dynamic_resources[new_id] = value
        logging.info(f"Created new resource {new_id} = {value}")
        return ResponseCode.CREATED, new_id
