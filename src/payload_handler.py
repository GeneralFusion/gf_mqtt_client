from enum import Enum
from typing import Dict, Any, Optional, Union, List
from pydantic import BaseModel, field_validator, ValidationError

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


class PayloadBaseModel(BaseModel):
    timestamp: Union[int, str]
    body: Optional[Union[int, float, str, List, Dict]] = None

    @field_validator('timestamp')
    def validate_timestamp(cls, v):
        if isinstance(v, str):
            return int(v) if v.isdigit() else v
        if isinstance(v, int):
            return v
        raise ValueError("timestamp must be an integer or string representation of an integer")
    
    @field_validator('body')
    def validate_body(cls, v):
        if v is None:
            return v
        if isinstance(v, (int, float, str, list, dict)):
            return v
        raise ValueError("body must be an int, float, str, list, or dict")
# Pydantic Models
class RequestBaseModel(BaseModel):
    request_id: str

    @field_validator('request_id')
    def validate_request_id(cls, v):
        v_stripped = v.replace('-', '').strip()
        if not (len(v_stripped) == 32 and all(c in '0123456789abcdefABCDEF' for c in v_stripped)):
            raise ValueError("request_id must be a 32-character hexadecimal string")
        return v

class HeaderRequest(RequestBaseModel):
    method: Union[int, str]
    path: str
    token: Optional[str] = None
    correlation_id: Optional[str] = None

    @field_validator('method')
    def validate_method(cls, v):
        try:
            assert isinstance(v, (int, str)), "Method must be an integer (1-4) or string ('GET', 'POST', 'PUT', 'DELETE')"
            if isinstance(v, int):
                assert v in {m.value for m in Method}
                return Method(v).value
            if isinstance(v, str):
                assert v in {m.name for m in Method}
                return Method[v.upper()].value
        except AssertionError:
            raise ValueError("Method must be an integer (1-4) or string ('GET', 'POST', 'PUT', 'DELETE')")
        
class HeaderResponse(RequestBaseModel):
    response_code: int
    path: str
    correlation_id: Optional[str] = None

    @field_validator('response_code')
    def validate_response_code(cls, v):
        return ResponseCode(v).value if v in {rc.value for rc in ResponseCode} else v

class GeneralPayload(PayloadBaseModel):
    pass

class RequestPayload(PayloadBaseModel):
    header: HeaderRequest

class ResponsePayload(PayloadBaseModel):
    header: HeaderResponse

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
                              correlation_id: Optional[str] = None) -> Dict[str, Any]:
        header = HeaderRequest(method=method.value, path=path, request_id=request_id, token=token, correlation_id=correlation_id)
        payload = RequestPayload(header=header, body=body, timestamp=str(self._get_current_timestamp()))
        return payload.model_dump()

    def create_response_payload(self, response_code: ResponseCode, path: str, request_id: str,
                               body: Dict[str, Any], correlation_id: Optional[str] = None) -> Dict[str, Any]:
        header = HeaderResponse(response_code=response_code.value, path=path, request_id=request_id, correlation_id=correlation_id)
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