"""
Protocol Utilities for MQTT Message Validation.

This module provides utility functions for validating protocol elements such as
methods and response codes. These utilities support input validation and can be
used for message inspection and debugging.

Functions:
    - is_valid_method(): Check if an integer is a valid Method code
    - is_valid_response_code(): Check if an integer is a valid ResponseCode

These utilities are lightweight validation helpers that complement the Pydantic
validation in the models module.

Example:
    >>> from gf_mqtt_client.core.protocol_utils import ProtocolUtils
    >>> ProtocolUtils.is_valid_method(1)  # GET
    True
    >>> ProtocolUtils.is_valid_method(999)
    False
"""
from enum import Enum
from .models import Method, ResponseCode


class ProtocolUtils:
    """
    Utility class for protocol validation and inspection.

    Provides static methods for validating method codes and response codes
    against the protocol specification. Useful for input validation, debugging,
    and protocol conformance checking.
    """

    @staticmethod
    def is_valid_method(method: int) -> bool:
        """
        Check if an integer represents a valid Method code.

        Validates whether the given integer corresponds to one of the defined
        HTTP-like methods (GET=1, POST=2, PUT=3, DELETE=4).

        Args:
            method: Integer to validate as a method code

        Returns:
            True if method is valid (1-4), False otherwise

        Example:
            >>> ProtocolUtils.is_valid_method(1)  # GET
            True
            >>> ProtocolUtils.is_valid_method(2)  # POST
            True
            >>> ProtocolUtils.is_valid_method(999)  # Invalid
            False
        """
        return method in [m.value for m in Method]

    @staticmethod
    def is_valid_response_code(code: int) -> bool:
        """
        Check if an integer represents a valid ResponseCode.

        Validates whether the given integer corresponds to one of the defined
        CoAP-style response codes (2xx success, 4xx client error, 5xx server error).

        Args:
            code: Integer to validate as a response code

        Returns:
            True if code is a known ResponseCode, False otherwise

        Example:
            >>> ProtocolUtils.is_valid_response_code(205)  # CONTENT
            True
            >>> ProtocolUtils.is_valid_response_code(404)  # NOT_FOUND
            True
            >>> ProtocolUtils.is_valid_response_code(999)  # Invalid
            False

        Note:
            This only checks against defined response codes. Unknown codes may
            still be valid in future protocol versions.
        """
        return code in [rc.value for rc in ResponseCode]
