import asyncio
import threading
from time import sleep, time
from typing import Any, Dict, Optional, List
from concurrent.futures import ThreadPoolExecutor
import atexit

from gf_mqtt_client.exceptions import ResponseException
from gf_mqtt_client.models import Method
from gf_mqtt_client.mqtt_client import MQTTClient
from gf_mqtt_client.message_handler import MessageHandlerProtocol


class SyncMQTTClient:
    """
    Synchronous wrapper for the async MQTTClient.
    Provides blocking methods that can be used in non-async code.
    """

    def __init__(self,
                 broker: str, 
                 port: int = 1883, 
                 timeout: int = 5, 
                 identifier: Optional[str] = None, 
                 subscriptions: Optional[list] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 ensure_unique_identifier: bool = False,
                 qos_default: Optional[int] = 0
                 ) -> None:
        """
        Initialize the synchronous MQTT client wrapper.
        
        Args:
            broker: MQTT broker hostname
            port: MQTT broker port (default: 1883)
            timeout: Request timeout in seconds (default: 5)
            identifier: Client identifier (auto-generated if None)
            subscriptions: List of topics to subscribe to
            username: MQTT username (optional)
            password: MQTT password (optional)
        """
        self._mqtt_client = MQTTClient(
            broker=broker,
            port=port,
            timeout=timeout,
            identifier=identifier,
            subscriptions=subscriptions,
            username=username,
            password=password,
            ensure_unique_identifier=ensure_unique_identifier,
            qos_default=qos_default
        )

        self._loop = None
        self._loop_thread = None
        self._connected = False
        self._grace_period = 0.5  # seconds
        self._default_timeout = timeout

        # Register cleanup on exit
        atexit.register(self._cleanup)

    def _start_event_loop(self):
        """Start the asyncio event loop in a separate thread."""
        def run_loop():
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            try:
                self._loop.run_forever()
            finally:
                self._loop.close()

        self._loop_thread = threading.Thread(target=run_loop, daemon=True)
        self._loop_thread.start()

        # Wait for the loop to be ready
        while self._loop is None:
            threading.Event().wait(0.001)

    def _ensure_loop_running(self):
        """Ensure the event loop is running."""
        if self._loop is None or not self._loop.is_running():
            self._start_event_loop()

    def _run_async(self, coro, timeout=None):
        """Run an async coroutine and return the result."""
        self._ensure_loop_running()
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=timeout)

    def connect(self):
        """Connect to the MQTT broker (blocking)."""
        self._run_async(self._mqtt_client.connect())
        self._connected = True
        return self

    def disconnect(self):
        """Disconnect from the MQTT broker (blocking)."""
        if self._connected:
            self._run_async(self._mqtt_client.disconnect())
            self._connected = False

    def request(self, target_device_tag, subsystem, path: str, method: Method = Method.GET, value: Any = None, timeout: int|None = None, qos: Optional[int] = 0) -> Optional[Dict[str, Any]]:
        """
        Send a request and wait for response (blocking).
        _grace_period of 0.5 seconds is added to timeout for improved reliability.
        This ensures that the correct ResponseException is raised on timeout, if a timeout is specified for the request.

        Args:
            target_device_tag: Target device identifier
            subsystem: Subsystem name
            path: Request path

        Returns:
            Response payload or None if timeout/error
        """
        if not self._connected:
            raise RuntimeError("Client not connected. Call connect() first.")
        effective_timeout = (timeout or self._default_timeout)
        if effective_timeout is not None:
            effective_timeout += self._grace_period
        try:
            return self._run_async(
                self._mqtt_client.request(target_device_tag=target_device_tag, subsystem=subsystem, path=path, method=method, value=value, timeout=timeout, qos=qos),
                timeout=effective_timeout
            )
        except ResponseException as e:
            self._mqtt_client.logger.warning(
                "Device protocol error %s:%s:%s: %s",
                target_device_tag, subsystem, path, e,
                exc_info=e,  # include traceback
            )
            raise  # re-raise the original exception unchanged

        except Exception as e:
            self._mqtt_client.logger.error(
                "Transport/internal failure %s:%s:%s: %s",
                target_device_tag, subsystem, path, e,
                exc_info=e,
            )
            raise

    def publish(self, topic: str, payload: Dict[str, Any], qos: int = 0):
        """Publish a message (blocking)."""
        if not self._connected:
            raise RuntimeError("Client not connected. Call connect() first.")

        self._run_async(self._mqtt_client.publish(topic, payload, qos))

    def subscribe(self, topic: str, qos: int = 0):
        """Subscribe to a topic (blocking)."""
        if not self._connected:
            raise RuntimeError("Client not connected. Call connect() first.")

        self._run_async(self._mqtt_client.subscribe(topic, qos=qos))

    def add_message_handler(self, handler: MessageHandlerProtocol):
        """Add a message handler (blocking)."""
        self._run_async(self._mqtt_client.add_message_handler(handler))

    def remove_message_handler(self, handler: MessageHandlerProtocol):
        """Remove a message handler (blocking)."""
        self._run_async(self._mqtt_client.remove_message_handler(handler))

    def set_credentials(self, username: str, password: str):
        """Set MQTT credentials."""
        self._mqtt_client.set_credentials(username, password)

    @property
    def identifier(self) -> str:
        """Get the client identifier."""
        return self._mqtt_client.identifier

    @property
    def is_connected(self) -> bool:
        """Check if the client is connected."""
        return self._connected

    def _cleanup(self):
        """Cleanup resources on exit."""
        if self._connected:
            self.disconnect()

        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)

        if self._loop_thread and self._loop_thread.is_alive():
            self._loop_thread.join(timeout=1.0)

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
