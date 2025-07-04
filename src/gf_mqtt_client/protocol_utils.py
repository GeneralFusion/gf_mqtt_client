from enum import Enum
from src.payload_handler import Method, ResponseCode

class ProtocolUtils:
    @staticmethod
    def is_valid_method(method: int) -> bool:
        return method in [m.value for m in Method]

    @staticmethod
    def is_valid_response_code(code: int) -> bool:
        return code in [rc.value for rc in ResponseCode]
