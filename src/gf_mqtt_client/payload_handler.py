from enum import Enum
from typing import Dict, Any, Optional
from pydantic import ValidationError
from .models import GeneralPayload, RequestPayload, ResponsePayload, HeaderRequest, HeaderResponse, Method, ResponseCode


class PayloadHandler:
    def __init__(self):
        self.methods = Method
        self.response_codes = ResponseCode

    def create_general_payload(self, body: Dict[str, Any], timestamp: str) -> Dict[str, Any]:
        payload = GeneralPayload(body=body, timestamp=timestamp)
        return payload.model_dump()

    def create_request_payload(self, method: Method, path: str, request_id: str,
                              body: Optional[Dict[str, Any]] = None,
                              token: Optional[str] = None,
                              correlation_id: Optional[str] = None,
                              source: Optional[str] = None) -> Dict[str, Any]:
        header = HeaderRequest(method=method.value, path=path, request_id=request_id, token=token, correlation_id=correlation_id, source=source)
        payload = RequestPayload(header=header, body=body, timestamp=str(self._get_current_timestamp()))
        return payload.model_dump()

    def create_response_payload(self, response_code: ResponseCode, path: str, request_id: str,
                               body: Dict[str, Any], correlation_id: Optional[str] = None, source: Optional[str] = None) -> Dict[str, Any]:
        header = HeaderResponse(response_code=response_code.value, path=path, request_id=request_id, correlation_id=correlation_id, source=source)
        payload = ResponsePayload(header=header, body=body, timestamp=str(self._get_current_timestamp()))
        return payload.model_dump()

    def validate_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if "method" in payload.get("header", {}):
                return RequestPayload(**payload).model_dump()
            elif "response_code" in payload.get("header", {}):
                return ResponsePayload(**payload).model_dump()
            else:
                return GeneralPayload(**payload).model_dump()
        except ValidationError as e:
            print(f"Validation error: {e.json()}")
            raise
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            raise

    def parse_payload(self, payload: str) -> Dict[str, Any]:
        import json
        try:
            payload_dict = json.loads(payload)
            return self.validate_payload(payload_dict)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON payload")

    def _get_current_timestamp(self) -> int:
        from time import time
        return int(time() * 1000)