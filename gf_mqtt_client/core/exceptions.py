from typing import Optional


class ResponseException(Exception):
    """
    Base for all response‑related errors. Carries:
      - response_code: numeric status (e.g. 404, 500)
      - path: the URI or topic requested
      - detail: any payload or error message returned
      - source: who sent it (e.g. 'broker', 'handler')
      - target: the intended recipient or device tag
    """
    default_code: Optional[int] = None

    def __init__(
        self,
        detail: Optional[str] = None,
        *,
        path: Optional[str] = None,
        source: Optional[str] = None,
        target: Optional[str] = None,
        response_code: Optional[int] = None,
        request_id: Optional[str] = None,
        method: Optional[str] = None

    ):
        # pick the explicit response_code or fall back to subclass default
        self.response_code = response_code if response_code is not None else self.default_code
        self.path = path
        self.detail = detail
        self.source = source
        self.target = target
        self.request_id = request_id
        self.method = method

        parts = [f"code={self.response_code}"]
        if path:
            parts.append(f"path={path!r}")
        if source:
            parts.append(f"source={source!r}")
        if target:
            parts.append(f"target={target!r}")
        if request_id:
            parts.append(f"request_id={request_id!r}")
        if method:
            parts.append(f"method={method!r}")

        super().__init__(f"{detail!r} {self.__class__.__name__}: " + ", ".join(parts))

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"response_code={self.response_code!r}, "
            f"path={self.path!r}, "
            f"detail={self.detail!r}, "
            f"source={self.source!r}, "
            f"target={self.target!r}, "
            f"request_id={self.request_id!r}, "
            f"method={self.method!r}"
            f")"
        )


class BadRequestResponse(ResponseException):
    """Request is malformed or contains invalid parameters (400)."""

    default_code = 400


class UnauthorizedResponse(ResponseException):
    """Authentication is required or failed (401)."""

    default_code = 401


class BadOptionResponse(ResponseException):
    """Request contains an unrecognized, malformed, or invalid option (402)."""

    default_code = 402


class ForbiddenResponse(ResponseException):
    """Request is understood but explicitly refused by the server (403)."""

    default_code = 403


class NotFoundResponse(ResponseException):
    """Requested resource or path was not found (404)."""

    default_code = 404


class MethodNotAllowedResponse(ResponseException):
    """Request method is not supported for the target resource (405)."""

    default_code = 405


class NotAcceptableResponse(ResponseException):
    """Requested content format or parameters are not acceptable (406)."""

    default_code = 406


class PreconditionFailedResponse(ResponseException):
    """Client preconditions were not met, e.g. ETag mismatch (412)."""

    default_code = 412


class RequestEntityTooLargeResponse(ResponseException):
    """Request payload is too large for the server to process (413)."""

    default_code = 413


class UnsupportedContentFormatResponse(ResponseException):
    """Request payload format is not supported by the server (415)."""

    default_code = 415


class InternalServerErrorResponse(ResponseException):
    """Generic server error while processing the request (500)."""

    default_code = 500


class NotImplementedResponse(ResponseException):
    """Server does not support the requested method or feature (501)."""

    default_code = 501


class BadGatewayResponse(ResponseException):
    """Invalid response received from upstream or intermediary (502)."""

    default_code = 502


class ServiceUnavailableResponse(ResponseException):
    """Server is temporarily unavailable, busy, or down (503)."""

    default_code = 503


class GatewayTimeoutResponse(ResponseException):
    """Request timed out while awaiting response from upstream (504)."""

    default_code = 504


class ProxyingNotSupportedResponse(ResponseException):
    """Server does not support proxying requests (505)."""

    default_code = 505


__all__ = [
    "ResponseException",
    "BadRequestResponse",
    "UnauthorizedResponse",
    "BadOptionResponse",
    "ForbiddenResponse",
    "NotFoundResponse",
    "MethodNotAllowedResponse",
    "NotAcceptableResponse",
    "PreconditionFailedResponse",
    "RequestEntityTooLargeResponse",
    "UnsupportedContentFormatResponse",
    "InternalServerErrorResponse",
    "NotImplementedResponse",
    "BadGatewayResponse",
    "ServiceUnavailableResponse",
    "GatewayTimeoutResponse",
    "ProxyingNotSupportedResponse",
]
