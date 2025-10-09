import logging
import time
from typing import Any, Dict
from gf_mqtt_client import Method, ResponseCode



class MockMQTTDevice:
    identifier = "mock_device"

    def __init__(self):
        self.gains = [0, 1, 2, 3, 4]
        self.sample_rate = 5000
        self.status = {"last_update": time.time(), "state": "IDLE"}
        self.ip = "10.10.10.10"
        self.firmware_version = "0.0.1"

        self.dynamic_resources: Dict[str, Any] = {}
        self.dynamic_counter = 0

        self._writable = {"gains", "sample_rate"}
        self._readable = {"gains", "sample_rate", "status", "ip", "firmware_version"}
        self._actions = {"arm", "trigger"}
        self._collections = {"resources"}

        self._errors = {
            "bad_option": ResponseCode.BAD_OPTION,
            "forbidden": ResponseCode.FORBIDDEN,
            "not_acceptable": ResponseCode.NOT_ACCEPTABLE,
            "precondition_failed": ResponseCode.PRECONDITION_FAILED,
            "request_entity_too_large": ResponseCode.REQUEST_ENTITY_TOO_LARGE,
            "unsupported_content_format": ResponseCode.UNSUPPORTED_CONTENT_FORMAT,
            "not_implemented": ResponseCode.NOT_IMPLEMENTED,
            "bad_gateway": ResponseCode.BAD_GATEWAY,
            "service_unavailable": ResponseCode.SERVICE_UNAVAILABLE,
            "proxying_not_supported": ResponseCode.PROXYING_NOT_SUPPORTED,
        }

    def handle_request(self, request: dict) -> dict:
        header = request["header"]
        path = header["path"]
        method = header["method"]
        request_id = header["request_id"]
        body = request.get("body")
        correlation_id = header.get("correlation_id")

        # Simulate error conditions
        if path in self._errors:
            return self._build_response(
                path, request_id, correlation_id, self._errors[path]
            )

        # Dispatch based on method
        if method == Method.GET.value:
            code, data = self._handle_get(path)
        elif method == Method.PUT.value:
            code = self._handle_put(path, body)
            data = None
        elif method == Method.POST.value:
            code, data = self._handle_post(path, body)
        else:
            code, data = ResponseCode.NOT_IMPLEMENTED, None

        return self._build_response(
            path=path,
            request_id=request_id,
            correlation_id=correlation_id,
            response_code=code,
            body=data,
            location=data if code == ResponseCode.CREATED else None,
        )

    def _handle_get(self, path: str):
        if path in self.dynamic_resources:
            return ResponseCode.CONTENT, self.dynamic_resources[path]
        if path in self._readable:
            return ResponseCode.CONTENT, getattr(self, path)
        return ResponseCode.NOT_FOUND, None

    def _handle_put(self, path: str, value: Any) -> ResponseCode:
        if path in self._writable:
            setattr(self, path, value)
            logging.info(f"Updated {path} → {value}")
            return ResponseCode.CHANGED
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
            method()
            return ResponseCode.VALID
        return ResponseCode.NOT_FOUND

    def _create_resource(self, collection: str, value: Any) -> (ResponseCode, str):
        new_path = f"{collection}/{self.dynamic_counter}"
        self.dynamic_counter += 1
        self.dynamic_resources[new_path] = value
        logging.info(f"Created resource {new_path} = {value}")
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
            "source": self.identifier,
        }
        if location:
            header["location"] = location
        return {
            "header": header,
            "body": body,
            "timestamp": str(int(time.time() * 1000)),
        }

    def arm(self):
        self._set_state("ARMED")

    def trigger(self):
        if self.status["state"] == "ARMED":
            self._set_state("TRIGGERED")
        else:
            logging.warning("Trigger rejected: device not armed.")

    def _set_state(self, state: str):
        self.status = {"last_update": time.time(), "state": state}
        logging.info(f"Device state set to: {state}")
