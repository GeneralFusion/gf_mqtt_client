import logging
from typing import Dict, Any, Optional, List, Callable, Union
from enum import Enum
from dataclasses import dataclass, field
import json


class MessageDirection(Enum):
    UNKNOWN = "unknown"
    REQUEST = "request"
    RESPONSE = "response"
    INCOMING = "incoming"
    OUTGOING = "outgoing"


# ============================================================================
# LIBRARY SIDE: Your MQTT module only needs this minimal interface
# ============================================================================


class MQTTLoggerMixin:
    """
    Mixin for MQTT clients that provides structured logging without formatting.

    Your MQTT client inherits from this and just calls the logging methods
    with structured extra data. Users configure formatting externally.
    """

    def __init__(
        self,
        *args,
        logger_name: str = None,
        base_logger: logging.Logger = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        # Use provided logger or create a standard one
        if base_logger:
            self.logger = base_logger
        else:
            self.logger = logging.getLogger(logger_name or self.__class__.__module__)

    def _log_with_context(
        self,
        level: int,
        message: str,
        extra: Optional[Dict[str, Any]] = None,
        **context_kwargs,
    ):
        """
        Internal method to add MQTT-specific context to log calls.

        Args:
            level: Logging level
            message: Log message
            extra: Additional context dict
            **context_kwargs: Context as keyword arguments
        """
        # Build structured context
        mqtt_context = {
            "mqtt_client_id": getattr(self, "identifier", "unknown"),
            "mqtt_component": "client",
            **context_kwargs,
        }

        # Merge with any additional extra context
        if extra:
            mqtt_context.update(extra)

        # Let the logging system handle formatting
        self.logger.log(level, message, extra=mqtt_context)

    def log_connection(
        self, message: str, success: bool = True, broker: str = None, port: int = None
    ):
        """Log connection events."""
        self._log_with_context(
            logging.INFO if success else logging.ERROR,
            message,
            mqtt_event_type="connection",
            mqtt_success=success,
            mqtt_broker=broker or getattr(self, "broker", None),
            mqtt_port=port or getattr(self, "port", None),
        )

    def log_message(
        self,
        message: str,
        direction: Union[str, MessageDirection],
        topic: str = None,
        payload: Any = None,
        request_id: str = None,
        source: str = None,
        target: str = None,
        subsystem: str = None,
        level: int = logging.INFO,
    ):
        """Log MQTT message events."""
        if isinstance(direction, MessageDirection):
            direction = direction.value

        self._log_with_context(
            level,
            message,
            mqtt_event_type="message",
            mqtt_direction=direction,
            mqtt_topic=topic,
            mqtt_payload=payload,
            mqtt_request_id=request_id,
            mqtt_source=source,
            mqtt_target=target,
            mqtt_subsystem=subsystem,
        )

    def log_request(
        self,
        message: str,
        target: str,
        subsystem: str = None,
        path: str = None,
        method: str = None,
        request_id: str = None,
        level: int = logging.INFO,
    ):
        """Log request events."""
        self._log_with_context(
            level,
            message,
            mqtt_event_type="request",
            mqtt_direction=MessageDirection.REQUEST.value,
            mqtt_target=target,
            mqtt_source=getattr(self, "identifier", "unknown"),
            mqtt_subsystem=subsystem,
            mqtt_path=path,
            mqtt_method=method,
            mqtt_request_id=request_id,
        )

    def log_response(
        self,
        message: str,
        source: str,
        request_id: str = None,
        response_code: int = None,
        subsystem: str = None,
        level: int = logging.INFO,
    ):
        """Log response events."""
        self._log_with_context(
            level,
            message,
            mqtt_event_type="response",
            mqtt_direction=MessageDirection.RESPONSE.value,
            mqtt_source=source,
            mqtt_target=getattr(self, "identifier", "unknown"),
            mqtt_request_id=request_id,
            mqtt_response_code=response_code,
            mqtt_subsystem=subsystem,
        )

    def log_error(
        self, message: str, error: Exception = None, context: Dict[str, Any] = None
    ):
        """Log error events."""
        error_context = {
            "mqtt_event_type": "error",
            "mqtt_error_type": type(error).__name__ if error else "unknown",
            "mqtt_error_message": str(error) if error else None,
        }

        if context:
            error_context.update(context)

        self._log_with_context(logging.ERROR, message, error_context)


# ============================================================================
# USER SIDE: Optional formatting utilities users can choose to use
# ============================================================================


@dataclass
class MQTTLogFormatConfig:
    """Configuration for MQTT log formatting that users can customize."""

    # Main format template
    format_template: str = (
        "%(asctime)s - %(name)s - %(levelname)s - {mqtt_tag}%(message)s"
    )

    # Tag template for MQTT-specific info
    tag_template: str = "[{client_id}{direction}]{route}{metadata} - "

    # Direction indicators
    direction_map: Dict[str, str] = field(
        default_factory=lambda: {
            "request": " / REQ →",
            "response": " / RESP ←",
            "incoming": " / IN ←",
            "outgoing": " / OUT →",
            "unknown": "",
        }
    )

    # Fields to show in metadata
    metadata_fields: List[str] = field(
        default_factory=lambda: ["mqtt_subsystem", "mqtt_request_id", "mqtt_method"]
    )

    # Field display names
    field_names: Dict[str, str] = field(
        default_factory=lambda: {
            "mqtt_subsystem": "subsystem",
            "mqtt_request_id": "req_id",
            "mqtt_method": "method",
            "mqtt_response_code": "code",
        }
    )

    # Maximum field value length
    max_field_length: int = 50

    # Whether to show the tag when no MQTT context is present
    show_tag_for_non_mqtt: bool = False


class MQTTLogFormatter(logging.Formatter):
    """
    Optional formatter that users can choose to use for MQTT-aware formatting.

    Users are free to implement their own formatters or use this one.
    """

    def __init__(self, config: MQTTLogFormatConfig = None):
        self.config = config or MQTTLogFormatConfig()
        # Don't call super().__init__() - we'll handle formatting completely

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record with MQTT context."""
        # Check if this is an MQTT-related log
        if hasattr(record, "mqtt_client_id"):
            mqtt_tag = self._build_mqtt_tag(record)
        elif self.config.show_tag_for_non_mqtt:
            mqtt_tag = "[NO-MQTT] - "
        else:
            mqtt_tag = ""

        # Build the base format string
        format_str = self.config.format_template.format(mqtt_tag=mqtt_tag)

        # Use standard formatter for the rest
        formatter = logging.Formatter(format_str)
        return formatter.format(record)

    def _build_mqtt_tag(self, record: logging.LogRecord) -> str:
        """Build the MQTT-specific tag portion."""
        client_id = getattr(record, "mqtt_client_id", "unknown")
        direction = self._get_direction_indicator(
            getattr(record, "mqtt_direction", None)
        )
        route = self._build_route(record)
        metadata = self._build_metadata(record)

        return self.config.tag_template.format(
            client_id=client_id, direction=direction, route=route, metadata=metadata
        )

    def _get_direction_indicator(self, direction: str) -> str:
        """Get formatted direction indicator."""
        if not direction:
            return ""
        return self.config.direction_map.get(direction.lower(), "")

    def _build_route(self, record: logging.LogRecord) -> str:
        """Build source → target route info."""
        source = getattr(record, "mqtt_source", None)
        target = getattr(record, "mqtt_target", None)

        if not source and not target:
            return ""

        return f" {source or '?'} → {target or '?'}"

    def _build_metadata(self, record: logging.LogRecord) -> str:
        """Build metadata portion."""
        parts = []

        for field in self.config.metadata_fields:
            value = getattr(record, field, None)
            if value is None:
                continue

            # Get display name
            display_name = self.config.field_names.get(
                field, field.replace("mqtt_", "")
            )

            # Truncate if needed
            value_str = str(value)
            if (
                self.config.max_field_length > 0
                and len(value_str) > self.config.max_field_length
            ):
                value_str = value_str[: self.config.max_field_length - 3] + "..."

            parts.append(f"{display_name}={value_str}")

        return f" | {' | '.join(parts)}" if parts else ""


class StructuredMQTTFormatter(logging.Formatter):
    """
    Structured (JSON) formatter for MQTT logs.

    Outputs JSON with separate fields for easy parsing by log aggregators.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Format as structured JSON."""
        log_data = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add MQTT-specific fields if present
        mqtt_fields = {}
        for attr_name in dir(record):
            if attr_name.startswith("mqtt_"):
                value = getattr(record, attr_name)
                if value is not None:
                    # Clean up the field name
                    field_name = attr_name.replace("mqtt_", "")
                    mqtt_fields[field_name] = value

        if mqtt_fields:
            log_data["mqtt"] = mqtt_fields

        # Add any other extra fields
        for key, value in record.__dict__.items():
            if (
                key not in log_data
                and not key.startswith("mqtt_")
                and not key.startswith(
                    (
                        "name",
                        "msg",
                        "args",
                        "levelname",
                        "levelno",
                        "pathname",
                        "filename",
                        "module",
                        "lineno",
                        "funcName",
                        "created",
                        "msecs",
                        "relativeCreated",
                        "thread",
                        "threadName",
                        "processName",
                        "process",
                        "stack_info",
                        "exc_info",
                        "exc_text",
                    )
                )
            ):
                log_data["extra"] = log_data.get("extra", {})
                log_data["extra"][key] = value

        return json.dumps(log_data, default=str)


# ============================================================================
# CONVENIENCE FUNCTIONS for users
# ============================================================================


def setup_mqtt_logging(
    logger_name: str,
    level: int = logging.INFO,
    format_style: str = "structured",  # "structured", "formatted", or "simple"
    config: MQTTLogFormatConfig = None,
) -> logging.Logger:
    """
    Convenience function for users to set up MQTT-aware logging.

    Args:
        logger_name: Name of the logger to configure
        level: Logging level
        format_style: "structured" (JSON), "formatted" (MQTT-aware), or "simple"
        config: Custom format configuration (only used with "formatted")

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    # Create handler
    handler = logging.StreamHandler()
    handler.setLevel(level)

    # Set formatter based on style
    if format_style == "structured":
        formatter = StructuredMQTTFormatter()
    elif format_style == "formatted":
        formatter = MQTTLogFormatter(config)
    else:  # simple
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


# ============================================================================
# EXAMPLE: How your MQTT client would be implemented
# ============================================================================


class ExampleMQTTClient(MQTTLoggerMixin):
    """Example of how your MQTT client would use the logging mixin."""

    def __init__(self, broker: str, identifier: str, logger: logging.Logger = None):
        # Initialize with logging support
        super().__init__(logger_name=f"mqtt.client.{identifier}", base_logger=logger)

        self.broker = broker
        self.identifier = identifier
        self.port = 1883

    async def connect(self):
        """Example connect method."""
        try:
            # Simulate connection
            self.log_connection("Attempting connection", success=True)
            # ... actual connection logic ...
            self.log_connection("Successfully connected to broker")
        except Exception as e:
            self.log_connection("Failed to connect to broker", success=False)
            self.log_error("Connection error", error=e)

    async def request(
        self, target: str, subsystem: str, path: str, method: str = "GET"
    ):
        """Example request method."""
        request_id = "req-12345"

        self.log_request(
            "Sending request",
            target=target,
            subsystem=subsystem,
            path=path,
            method=method,
            request_id=request_id,
        )

        # Simulate sending
        self.log_message(
            "Published request message",
            direction=MessageDirection.OUTGOING,
            topic=f"requests/{target}/{subsystem}",
            request_id=request_id,
        )

    async def _handle_response(self, payload: dict):
        """Example response handler."""
        request_id = payload.get("request_id")
        source = payload.get("source")

        self.log_response(
            "Received response",
            source=source,
            request_id=request_id,
            response_code=payload.get("code", 200),
        )


# ============================================================================
# EXAMPLE USAGE for users
# ============================================================================

if __name__ == "__main__":
    # Example 1: User sets up structured logging
    logger = setup_mqtt_logging("mqtt.example", format_style="structured")
    client = ExampleMQTTClient("localhost", "client-001", logger=logger)

    print("=== Structured JSON Logging ===")
    import asyncio

    asyncio.run(client.connect())
    asyncio.run(client.request("device-001", "sensors", "/temperature"))

    print("\n=== Formatted MQTT Logging ===")
    # Example 2: User sets up formatted logging with custom config
    custom_config = MQTTLogFormatConfig(
        metadata_fields=["mqtt_request_id", "mqtt_method", "mqtt_subsystem"],
        max_field_length=20,
    )
    logger2 = setup_mqtt_logging(
        "mqtt.example2", format_style="formatted", config=custom_config
    )
    client2 = ExampleMQTTClient("localhost", "client-002", logger=logger2)

    asyncio.run(client2.connect())
    asyncio.run(client2.request("device-002", "actuators", "/switch", "POST"))

    print("\n=== Simple Logging (No MQTT formatting) ===")
    # Example 3: User wants simple logging
    logger3 = setup_mqtt_logging("mqtt.example3", format_style="simple")
    client3 = ExampleMQTTClient("localhost", "client-003", logger=logger3)

    asyncio.run(client3.connect())
    asyncio.run(client3.request("device-003", "sensors", "/humidity"))
