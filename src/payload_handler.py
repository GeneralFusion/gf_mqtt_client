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
