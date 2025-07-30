import logging
from typing import Optional
from aiomqtt import Topic

REQUEST_TOPIC = "request"
RESPONSE_TOPIC = "response"

class TopicManager:
    def __init__(self, namespace: str = "gf_int_v1"):
        self.namespace = namespace

    def build_request_topic(self, target_device_tag: str, subsystem: str, request_id: str) -> str:
        return f"{self.namespace}/{subsystem}/{REQUEST_TOPIC}/{target_device_tag}/{request_id}"

    def build_response_topic(self, request_topic: str | Topic, target_device_tag: Optional[str] = None) -> str:
        if isinstance(request_topic, Topic):
            request_topic = request_topic.value
        parts = request_topic.split('/')
        request_id = parts[-1]
        subsystem = parts[1]
        target_device_tag = target_device_tag or parts[3]
        logging.debug(f"Building response topic for request_id: {request_id}, subsystem: {subsystem}, target_device_tag: {target_device_tag}")
        return f"{self.namespace}/{subsystem}/{RESPONSE_TOPIC}/{target_device_tag}/{request_id}"
    
    def get_target_device_tag_from_topic(self, request_topic: str | Topic) -> Optional[str]:
        if isinstance(request_topic, Topic):
            request_topic = request_topic.value
        parts = request_topic.split('/')
        if len(parts) < 4:
            logging.warning(f"Invalid request topic format: {request_topic}")
            return None
        target_device_tag = parts[3]
        logging.debug(f"Extracted target device tag from request topic: {target_device_tag}")
        return target_device_tag
    
    def get_parts(self, topic: str | Topic) -> dict:
        if isinstance(topic, Topic):
            topic = topic.value
        parts = topic.split('/')
        if len(parts) < 4:
            logging.warning(f"Invalid topic format: {topic}")
            return {}
        return {
            "namespace": parts[0],
            "subsystem": parts[1],
            "type": parts[2],
            "target": parts[3],
            "request_id": parts[-1]
        }