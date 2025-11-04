"""
MQTT Topic Management and Construction.

This module provides the TopicManager class for building and parsing MQTT topics
following the project's standardized topic format:

    {namespace}/{subsystem}/{type}/{target}/{request_id}

Example topic:
    gf_int_v1/sensors/request/device-001/abc-123-def-456

The TopicManager handles:
    - Building request and response topics
    - Parsing topics to extract components
    - Extracting target device tags from topics
    - Topic format validation and logging

This ensures consistent topic structure across the entire application and
enables proper message routing in the MQTT request-response protocol.

Example:
    >>> manager = TopicManager(namespace="gf_int_v1")
    >>> topic = manager.build_request_topic(
    ...     target_device_tag="device-001",
    ...     subsystem="sensors",
    ...     request_id="abc-123"
    ... )
    >>> # Returns: "gf_int_v1/sensors/request/device-001/abc-123"
"""
import logging
from typing import Optional
from aiomqtt import Topic
from .models import MessageType

logger = logging.getLogger(__name__)


class TopicManager:
    """
    Manager for MQTT topic construction and parsing.

    Handles all topic-related operations following the standardized format:
        {namespace}/{subsystem}/{type}/{target}/{request_id}

    Attributes:
        namespace: The MQTT topic namespace (default: "gf_int_v1")
    """

    def __init__(self, namespace: str = "gf_int_v1"):
        """
        Initialize the TopicManager with a namespace.

        Args:
            namespace: Root namespace for all topics (default: "gf_int_v1")
        """
        self.namespace = namespace
        logger.debug(f"Initialized TopicManager with namespace: {namespace}")

    def build_request_topic(self, target_device_tag: str, subsystem: str, request_id: str) -> str:
        """
        Build an MQTT topic for a request message.

        Constructs a topic following the format:
            {namespace}/{subsystem}/request/{target_device_tag}/{request_id}

        Args:
            target_device_tag: Identifier of the target device (e.g., "device-001", "2D_XX_0_9999")
            subsystem: Subsystem name (e.g., "sensors", "actuators", "axuv")
            request_id: Unique request identifier (UUID format)

        Returns:
            Formatted request topic string

        Example:
            >>> manager.build_request_topic("device-001", "sensors", "abc-123")
            "gf_int_v1/sensors/request/device-001/abc-123"
        """
        topic = f"{self.namespace}/{subsystem}/{MessageType.REQUEST.value}/{target_device_tag}/{request_id}"
        logger.debug(
            f"Built request topic: {topic}",
            extra={"subsystem": subsystem, "target": target_device_tag, "request_id": request_id}
        )
        return topic

    def build_response_topic(self, request_topic: str | Topic, target_device_tag: Optional[str] = None) -> str:
        """
        Build an MQTT topic for a response message from a request topic.

        Parses the request topic to extract subsystem and request_id, then constructs
        a corresponding response topic. The response topic replaces 'request' with
        'response' in the type field.

        Args:
            request_topic: Original request topic (string or aiomqtt.Topic)
            target_device_tag: Optional override for target device tag
                              (defaults to value from request_topic)

        Returns:
            Formatted response topic string

        Raises:
            IndexError: If request_topic format is invalid

        Example:
            >>> request = "gf_int_v1/sensors/request/device-001/abc-123"
            >>> manager.build_response_topic(request)
            "gf_int_v1/sensors/response/device-001/abc-123"

        Note:
            This method assumes the request topic follows the standard format.
            Malformed topics will cause parsing errors.
        """
        # Convert aiomqtt.Topic to string if needed
        if isinstance(request_topic, Topic):
            request_topic = request_topic.value

        # Parse request topic components
        parts = request_topic.split('/')
        if len(parts) < 5:
            logger.warning(
                f"Request topic has insufficient parts for response topic building: {request_topic}. "
                f"Expected format: {self.namespace}/{{subsystem}}/request/{{target}}/{{request_id}}"
            )

        request_id = parts[-1]
        subsystem = parts[1]
        target_device_tag = target_device_tag or parts[3]

        # Build response topic
        response_topic = f"{self.namespace}/{subsystem}/{MessageType.RESPONSE.value}/{target_device_tag}/{request_id}"

        logger.debug(
            f"Built response topic from request topic: {request_topic} -> {response_topic}",
            extra={"subsystem": subsystem, "target": target_device_tag, "request_id": request_id}
        )

        return response_topic

    def get_target_device_tag_from_topic(self, request_topic: str | Topic) -> Optional[str]:
        """
        Extract the target device tag from an MQTT topic.

        Parses the topic to extract the target device identifier from the
        standard position (4th component, index 3).

        Args:
            request_topic: MQTT topic string or aiomqtt.Topic object

        Returns:
            Target device tag string, or None if topic format is invalid

        Example:
            >>> manager.get_target_device_tag_from_topic("gf_int_v1/sensors/request/device-001/abc-123")
            "device-001"
        """
        # Convert aiomqtt.Topic to string if needed
        if isinstance(request_topic, Topic):
            request_topic = request_topic.value

        # Parse topic and extract target
        parts = request_topic.split('/')
        if len(parts) < 4:
            logger.warning(
                f"Invalid topic format, cannot extract target device tag: {request_topic}. "
                f"Expected at least 4 components: {{namespace}}/{{subsystem}}/{{type}}/{{target}}"
            )
            return None

        target_device_tag = parts[3]
        logger.debug(
            f"Extracted target device tag '{target_device_tag}' from topic: {request_topic}",
            extra={"target": target_device_tag}
        )
        return target_device_tag

    def get_parts(self, topic: str | Topic) -> dict:
        """
        Parse an MQTT topic into its component parts.

        Splits the topic string and returns a dictionary with labeled components
        following the standard format.

        Args:
            topic: MQTT topic string or aiomqtt.Topic object

        Returns:
            Dictionary with keys: namespace, subsystem, type, target, request_id
            Returns empty dict if topic format is invalid

        Example:
            >>> manager.get_parts("gf_int_v1/sensors/request/device-001/abc-123")
            {
                "namespace": "gf_int_v1",
                "subsystem": "sensors",
                "type": "request",
                "target": "device-001",
                "request_id": "abc-123"
            }
        """
        # Convert aiomqtt.Topic to string if needed
        if isinstance(topic, Topic):
            topic = topic.value

        # Parse topic components
        parts = topic.split('/')
        if len(parts) < 5:
            logger.warning(
                f"Invalid topic format, cannot parse components: {topic}. "
                f"Expected format: {{namespace}}/{{subsystem}}/{{type}}/{{target}}/{{request_id}}"
            )
            return {}

        parsed = {
            "namespace": parts[0],
            "subsystem": parts[1],
            "type": parts[2],
            "target": parts[3],
            "request_id": parts[-1]
        }

        logger.debug(f"Parsed topic components: {parsed}", extra=parsed)
        return parsed
