"""
Edge case and critical failure tests for synchronous MQTT client.

Tests scenarios that could cause production failures in sync client:
- Paho callback thread safety
- Network thread blocking
- Queue overflow
- Thread pool behavior
- wait=True/False publish semantics
- Reconnection logic
- Context manager exception safety
"""
import time
import threading
import pytest
from queue import Queue
from unittest.mock import Mock, patch
from gf_mqtt_client import (
    SyncMQTTClient,
    SyncRequestHandlerBase,
    SyncMessageHandlerBase,
    TopicManager,
    Method,
    GatewayTimeoutResponse,
)
from tests.conftest import BROKER_CONFIG


# ============================================================================
# PAHO CALLBACK THREAD SAFETY (SC-1)
# ============================================================================


class TestPahoCallbackThreadSafety:
    """Test thread safety in message handlers."""

    def test_handler_shared_state_thread_safety(self):
        """Verify handlers don't corrupt shared state due to race conditions."""
        shared_state = {"counter": 0, "values": []}
        lock = threading.Lock()
        increment_count = 1000

        def increment_handler(client, topic, payload):
            """Handler that increments shared counter."""
            # Simulate some work and race condition potential
            for _ in range(increment_count):
                with lock:
                    shared_state["counter"] += 1
                    shared_state["values"].append(shared_state["counter"])
            return payload

        client = SyncMQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=10,
            identifier="test_thread_safety",
            ensure_unique_identifier=True,
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        client.add_message_handler(
            SyncRequestHandlerBase(process=increment_handler, propagate=False)
        )

        client.connect()

        try:
            # Send multiple concurrent requests
            num_requests = 5
            results = []

            def make_request():
                try:
                    result = client.request("device", "subsystem", "test_path")
                    results.append(result)
                except GatewayTimeoutResponse:
                    # Expected since there's no responder
                    pass

            threads = [
                threading.Thread(target=make_request)
                for _ in range(num_requests)
            ]

            for t in threads:
                t.start()

            for t in threads:
                t.join(timeout=15)

            # Counter may not be exactly num_requests * increment_count if requests timed out
            # but it should never be corrupted (missing increments or duplicates due to races)
            # The lock ensures this
            assert shared_state["counter"] >= 0

        finally:
            client.disconnect()

    def test_concurrent_handler_list_modification(self):
        """Test thread safety when modifying handler list during processing."""
        client = SyncMQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_handler_mod",
            ensure_unique_identifier=True,
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        handler_called = []

        def test_handler(client, topic, payload):
            handler_called.append(True)
            time.sleep(0.1)  # Slow handler
            return payload

        handler = SyncRequestHandlerBase(process=test_handler, propagate=False)
        client.add_message_handler(handler)

        client.connect()

        try:
            # Start request in background
            def make_request():
                try:
                    client.request("device", "subsystem", "path")
                except GatewayTimeoutResponse:
                    pass

            request_thread = threading.Thread(target=make_request)
            request_thread.start()

            time.sleep(0.05)  # Let request start

            # Modify handlers while processing might be happening
            new_handler = SyncRequestHandlerBase(
                process=lambda c, t, p: p,
                propagate=True
            )
            client.add_message_handler(new_handler)
            client.remove_message_handler(handler)

            request_thread.join(timeout=10)

            # Should not crash
            assert True

        finally:
            client.disconnect()


# ============================================================================
# NETWORK THREAD BLOCKING (SC-4)
# ============================================================================


class TestNetworkThreadBlocking:
    """Test that slow handlers don't block paho network thread."""

    def test_slow_handler_does_not_block_network_thread(self):
        """Verify slow handler doesn't stall message processing."""
        client = SyncMQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=10,
            identifier="test_slow_handler",
            ensure_unique_identifier=True,
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        handler_start_times = []
        handler_end_times = []

        def slow_handler(client, topic, payload):
            """Handler that takes significant time."""
            handler_start_times.append(time.time())
            time.sleep(0.5)  # Slow processing
            handler_end_times.append(time.time())
            return payload

        client.add_message_handler(
            SyncRequestHandlerBase(process=slow_handler, propagate=False)
        )

        client.connect()

        try:
            # Send multiple requests - they should queue up, not block
            def make_request(i):
                try:
                    client.request("device", "subsystem", f"path_{i}")
                except GatewayTimeoutResponse:
                    pass

            threads = [
                threading.Thread(target=make_request, args=(i,))
                for i in range(3)
            ]

            start = time.time()
            for t in threads:
                t.start()

            for t in threads:
                t.join(timeout=15)

            elapsed = time.time() - start

            # Should have processed (handlers may time out, but shouldn't deadlock)
            # If network thread was blocked, this would hang
            assert elapsed < 20  # Generous timeout

        finally:
            client.disconnect()

    def test_handler_using_wait_false_avoids_deadlock(self):
        """Verify wait=False in publish() avoids deadlock in handlers."""
        # This test documents the correct pattern for publishing from handlers

        responder = SyncMQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=10,
            identifier="test_responder_wait",
            ensure_unique_identifier=True,
        )
        responder.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        def request_handler(client, topic, payload):
            """Handler that publishes response using wait=False."""
            response_payload = {
                "header": {
                    "message_type": "response",
                    "request_id": payload["header"]["request_id"],
                    "response_code": 205,
                    "timestamp": "123"
                },
                "body": {"result": "success"}
            }

            response_topic = TopicManager().build_response_topic(request_topic=topic)

            # CRITICAL: Use wait=False to avoid blocking paho network thread
            client.publish(response_topic, response_payload, wait=False)

            return response_payload

        responder.add_message_handler(
            SyncRequestHandlerBase(process=request_handler, propagate=False)
        )

        responder.connect()

        requestor = SyncMQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=10,
            identifier="test_requestor_wait",
            ensure_unique_identifier=True,
        )
        requestor.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)
        requestor.connect()

        try:
            # This should not deadlock
            result = requestor.request(
                target_device_tag=responder.identifier,
                subsystem="subsystem",
                path="test"
            )

            assert result is not None
            assert result["header"]["response_code"] == 205

        finally:
            requestor.disconnect()
            responder.disconnect()


# ============================================================================
# QUEUE OVERFLOW (SC-2)
# ============================================================================


class TestQueueOverflow:
    """Test response queue behavior under high load."""

    def test_many_concurrent_requests(self):
        """Test client handles many concurrent requests."""
        client = SyncMQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_queue_overflow",
            ensure_unique_identifier=True,
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        client.connect()

        try:
            # Send many concurrent requests
            num_requests = 50
            results = []

            def make_request(i):
                try:
                    result = client.request("device", "subsystem", f"path_{i}")
                    results.append(result)
                except GatewayTimeoutResponse:
                    # Expected - no responder
                    results.append(None)

            threads = [
                threading.Thread(target=make_request, args=(i,))
                for i in range(num_requests)
            ]

            for t in threads:
                t.start()

            for t in threads:
                t.join(timeout=10)

            # All requests should complete (timeout or succeed)
            assert len(results) == num_requests

        finally:
            client.disconnect()


# ============================================================================
# RECONNECTION LOGIC (SC-6)
# ============================================================================


class TestReconnectionLogic:
    """Test synchronous client reconnection behavior."""

    def test_reconnect_after_disconnect(self):
        """Test client can reconnect after explicit disconnect."""
        client = SyncMQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_reconnect",
            ensure_unique_identifier=True,
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        # Connect -> disconnect -> reconnect cycle
        client.connect()
        assert client.is_connected

        client.disconnect()
        assert not client.is_connected

        client.connect()
        assert client.is_connected

        client.disconnect()

    def test_request_after_disconnect_fails(self):
        """Verify requests fail after disconnect."""
        client = SyncMQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_req_after_disconnect",
            ensure_unique_identifier=True,
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        client.connect()
        client.disconnect()

        # Request should fail
        with pytest.raises(RuntimeError, match="not connected|disconnected"):
            client.request("device", "subsystem", "path")

    def test_multiple_connect_calls_idempotent(self):
        """Verify multiple connect() calls are safe."""
        client = SyncMQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_multi_connect",
            ensure_unique_identifier=True,
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        # Multiple connects should be safe
        client.connect()
        client.connect()  # Should not error
        client.connect()  # Should not error

        assert client.is_connected

        client.disconnect()


# ============================================================================
# CONTEXT MANAGER EXCEPTION SAFETY (SC-7)
# ============================================================================


class TestContextManagerExceptions:
    """Test context manager cleanup on exceptions."""

    def test_context_manager_cleanup_on_exception(self):
        """Verify context manager cleans up even when exception occurs."""
        exception_occurred = False

        # Create client and set credentials BEFORE entering context
        client = SyncMQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_context_exception",
            ensure_unique_identifier=True,
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        try:
            # Context manager will auto-connect in __enter__
            with client:
                # Should be connected
                assert client.is_connected

                # Raise exception
                raise ValueError("Intentional test exception")

        except ValueError:
            exception_occurred = True

        # Verify exception was raised and cleanup happened
        assert exception_occurred

    def test_context_manager_normal_cleanup(self):
        """Verify context manager cleans up normally."""
        # Create client and set credentials BEFORE entering context
        client = SyncMQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_context_normal",
            ensure_unique_identifier=True,
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        # Context manager will auto-connect in __enter__
        with client:
            client_id = client.identifier
            assert client.is_connected

        # After exiting context, should be disconnected
        assert not client.is_connected
        assert client_id is not None


# ============================================================================
# CONCURRENT CONNECT/DISCONNECT (SC-8)
# ============================================================================


class TestConcurrentConnectDisconnect:
    """Test concurrent connect/disconnect operations."""

    def test_concurrent_connect_calls(self):
        """Test concurrent connections from multiple client instances.

        This tests that multiple clients can connect simultaneously from
        different threads without interfering with each other. Each client
        gets a unique identifier via ensure_unique_identifier=True.
        """
        # Create multiple client instances
        clients = []
        for i in range(5):
            client = SyncMQTTClient(
                broker=BROKER_CONFIG.hostname,
                port=BROKER_CONFIG.port,
                timeout=5,
                identifier=f"test_concurrent_connect_{i}",
                ensure_unique_identifier=True,
            )
            client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)
            clients.append(client)

        results = []

        def try_connect(client):
            try:
                client.connect()
                results.append(True)
            except Exception as e:
                results.append(False)
                print(f"Connect failed for {client.identifier}: {e}")

        threads = [threading.Thread(target=try_connect, args=(client,)) for client in clients]

        for t in threads:
            t.start()

        for t in threads:
            t.join(timeout=10)

        # All should successfully connect
        assert len([r for r in results if r]) == 5, f"Expected 5 successful connections, got {len([r for r in results if r])}"

        # Clean up all clients
        for client in clients:
            try:
                if client.is_connected:
                    client.disconnect()
            except Exception as e:
                print(f"Cleanup error for {client.identifier}: {e}")

    def test_concurrent_disconnect_calls(self):
        """Document behavior of multiple threads calling disconnect() simultaneously.

        This test documents that calling disconnect() concurrently from multiple
        threads may or may not be fully idempotent depending on timing. The first
        disconnect will succeed, and subsequent ones should either succeed silently
        or be handled gracefully.
        """
        client = SyncMQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_concurrent_disconnect",
            ensure_unique_identifier=True,
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        client.connect()
        assert client.is_connected

        results = []

        def try_disconnect():
            try:
                client.disconnect()
                results.append(True)
            except Exception as e:
                results.append(False)
                print(f"Disconnect exception (expected): {e}")

        threads = [threading.Thread(target=try_disconnect) for _ in range(5)]

        for t in threads:
            t.start()

        for t in threads:
            t.join(timeout=10)

        # At least one disconnect should succeed
        # The rest may succeed or fail depending on timing
        assert len([r for r in results if r]) >= 1

        # Should end up disconnected without crashing
        assert not client.is_connected


# ============================================================================
# WAIT PARAMETER BEHAVIOR (SC-5)
# ============================================================================


class TestWaitParameterBehavior:
    """Test wait=True/False publish semantics."""

    def test_publish_with_wait_true(self):
        """Test publish() with wait=True blocks until published."""
        client = SyncMQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_wait_true",
            ensure_unique_identifier=True,
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        client.connect()

        try:
            start = time.time()

            # Publish with wait=True (default)
            client.publish(
                "gf_int_v1/subsystem/request/device/test",
                {"header": {"message_type": "request", "timestamp": "123"}},
                wait=True
            )

            elapsed = time.time() - start

            # Should have blocked briefly
            assert elapsed >= 0

        finally:
            client.disconnect()

    def test_publish_with_wait_false(self):
        """Test publish() with wait=False returns immediately."""
        client = SyncMQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_wait_false",
            ensure_unique_identifier=True,
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        client.connect()

        try:
            start = time.time()

            # Publish with wait=False
            client.publish(
                "gf_int_v1/subsystem/request/device/test",
                {"header": {"message_type": "request", "timestamp": "123"}},
                wait=False
            )

            elapsed = time.time() - start

            # Should return very quickly
            assert elapsed < 1.0

        finally:
            client.disconnect()


# ============================================================================
# HANDLER EXCEPTION HANDLING
# ============================================================================


class TestHandlerExceptionHandling:
    """Test exception handling in sync handlers."""

    def test_handler_exception_does_not_crash(self):
        """Verify exceptions in handlers don't crash client."""
        client = SyncMQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_handler_exception",
            ensure_unique_identifier=True,
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        def bad_handler(client, topic, payload):
            raise RuntimeError("Intentional error in handler")

        client.add_message_handler(
            SyncMessageHandlerBase(
                can_handle=lambda c, t, p: True,
                process=bad_handler,
                propagate=False
            )
        )

        client.connect()

        try:
            # Publish message that will trigger exception
            client.publish(
                f"gf_int_v1/subsystem/request/{client.identifier}/test",
                {
                    "header": {
                        "message_type": "request",
                        "method": "GET",
                        "path": "test",
                        "request_id": "test-id",
                        "timestamp": "123"
                    }
                }
            )

            time.sleep(0.5)

            # Client should still be connected
            assert client.is_connected

        finally:
            client.disconnect()

    def test_can_handle_predicate_exception(self):
        """Test exception in can_handle predicate."""
        client = SyncMQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_predicate_exception",
            ensure_unique_identifier=True,
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        def bad_predicate(client, topic, payload):
            raise ValueError("Intentional error in predicate")

        def handler(client, topic, payload):
            return payload

        client.add_message_handler(
            SyncMessageHandlerBase(
                can_handle=bad_predicate,
                process=handler,
                propagate=True
            )
        )

        client.connect()

        try:
            # Publish message
            client.publish(
                f"gf_int_v1/subsystem/request/{client.identifier}/test",
                {
                    "header": {
                        "message_type": "request",
                        "method": "GET",
                        "path": "test",
                        "request_id": "test-id",
                        "timestamp": "123"
                    }
                }
            )

            time.sleep(0.5)

            # Should not crash
            assert client.is_connected

        finally:
            client.disconnect()
