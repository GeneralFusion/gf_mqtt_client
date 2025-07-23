
class ResponseException(Exception):
    """Base exception for response errors."""


class BadRequestResponse(ResponseException):
    pass


class UnauthorizedResponse(ResponseException):
    pass


class NotFoundResponse(ResponseException):
    pass


class MethodNotAllowedResponse(ResponseException):
    pass


class InternalServerErrorResponse(ResponseException):
    pass

class GatewayTimeoutResponse(ResponseException):
    pass