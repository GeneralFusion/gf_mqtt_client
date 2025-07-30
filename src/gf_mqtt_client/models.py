from enum import Enum
from typing import Dict, Optional, Union, List
from pydantic import BaseModel, field_validator

class MQTTBrokerConfig(BaseModel):
    username: str = ""
    password: str = ""
    hostname: str = "broker.emqx.io"
    port: int = 1883
    timeout: int = 5

class Method(Enum):
    GET = 1
    POST = 2
    PUT = 3
    DELETE = 4


class MessageDirection(Enum):
    OUTGOING = "request"
    INCOMING = "response"
    UNKNOWN = "unknown" or "N/A"


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

    @field_validator("timestamp")
    def validate_timestamp(cls, v):
        if isinstance(v, str):
            return int(v) if v.isdigit() else v
        if isinstance(v, int):
            return v
        raise ValueError(
            "timestamp must be an integer or string representation of an integer"
        )

    @field_validator("body")
    def validate_body(cls, v):
        if v is None:
            return v
        if isinstance(v, (int, float, str, list, dict)):
            return v
        raise ValueError("body must be an int, float, str, list, or dict")


# Pydantic Models
class RequestBaseModel(BaseModel):
    request_id: str

    @field_validator("request_id")
    def validate_request_id(cls, v):
        v_stripped = v.replace("-", "").strip()
        if not (
            len(v_stripped) == 32
            and all(c in "0123456789abcdefABCDEF" for c in v_stripped)
        ):
            raise ValueError("request_id must be a 32-character hexadecimal string")
        return v


class HeaderRequest(RequestBaseModel):
    method: Union[int, str]
    path: str
    token: Optional[str] = None
    correlation_id: Optional[str] = None
    source: Optional[str] = None

    @field_validator("method")
    def validate_method(cls, v):
        try:
            assert isinstance(
                v, (int, str)
            ), "Method must be an integer (1-4) or string ('GET', 'POST', 'PUT', 'DELETE')"
            if isinstance(v, int):
                assert v in {m.value for m in Method}
                return Method(v).value
            if isinstance(v, str):
                assert v in {m.name for m in Method}
                return Method[v.upper()].value
        except AssertionError:
            raise ValueError(
                "Method must be an integer (1-4) or string ('GET', 'POST', 'PUT', 'DELETE')"
            )


class HeaderResponse(RequestBaseModel):
    response_code: int
    path: str
    correlation_id: Optional[str] = None
    source: Optional[str] = None

    @field_validator("response_code")
    def validate_response_code(cls, v):
        return ResponseCode(v).value if v in {rc.value for rc in ResponseCode} else v


class GeneralPayload(PayloadBaseModel):
    pass


class RequestPayload(PayloadBaseModel):
    header: HeaderRequest


class ResponsePayload(PayloadBaseModel):
    header: HeaderResponse
