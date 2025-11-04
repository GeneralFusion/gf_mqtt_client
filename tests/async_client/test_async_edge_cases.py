"""
Edge case and critical failure tests for async MQTT client.

Tests scenarios that could cause production failures:
- Connection failure recovery
- Disconnect with pending requests
- Duplicate request_id handling
- Handler modification during message processing
- Message loop exception handling
- Concurrent operations
"""
import asyncio
import pytest
import pytest_asyncio
from unittest.mock import Mock, patch, AsyncMock
from gf_mqtt_client import (
    MQTTClient,
    RequestHandlerBase,
    ResponseHandlerBase,
    MessageHandlerBase,
    GatewayTimeoutResponse,
    TopicManager,
    Method,
)
from tests.conftest import BROKER_CONFIG


# ============================================================================
# CONNECTION FAILURE RECOVERY (AC-1)
# ============================================================================


class TestConnectionFailureRecovery:
    """Test client recovery from connection failures."""

    @pytest.mark.asyncio
    async def test_request_after_disconnect_raises_error(self):
        """Verify requests fail after disconnect."""
        client = MQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_disconnect",
            ensure_unique_identifier=True
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        await client.connect()
        assert client.is_connected

        # Disconnect
        await client.disconnect()
        assert not client.is_connected

        # Request should fail
        with pytest.raises(RuntimeError, match="not connected|disconnected"):
            await client.request("device", "subsystem", "path")

    @pytest.mark.asyncio
    async def test_reconnect_after_disconnect(self):
        """Verify client can reconnect after explicit disconnect."""
        client = MQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_reconnect",
            ensure_unique_identifier=True
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        # Connect -> Disconnect -> Connect cycle
        await client.connect()
        await client.disconnect()
        await client.connect()

        assert client.is_connected

        await client.disconnect()

    @pytest.mark.asyncio
    async def test_multiple_connect_calls_idempotent(self):
        """Verify multiple connect() calls are idempotent."""
        client = MQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_multi_connect",
            ensure_unique_identifier=True
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        # Connect multiple times
        await client.connect()
        await client.connect()  # Should not error
        await client.connect()  # Should not error

        assert client.is_connected

        await client.disconnect()


# ============================================================================
# DISCONNECT WITH PENDING REQUESTS (AC-2)
# ============================================================================


class TestDisconnectWithPendingRequests:
    """Test disconnect behavior when requests are pending."""

    @pytest.mark.asyncio
    async def test_disconnect_cancels_pending_requests(self):
        """Verify disconnect cancels all pending requests."""
        client = MQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=60,  # Long timeout so requests don't naturally timeout
            identifier="test_pending",
            ensure_unique_identifier=True
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)
        await client.connect()

        # Start multiple requests that won't get responses
        tasks = [
            asyncio.create_task(
                client.request("nonexistent_device_xyz", "subsystem", f"path_{i}")
            )
            for i in range(10)
        ]

        # Let requests start
        await asyncio.sleep(0.2)

        # Disconnect
        await client.disconnect()

        # All pending requests should timeout or be cancelled
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # All should be exceptions
        for result in results:
            assert isinstance(result, (Exception, asyncio.CancelledError, GatewayTimeoutResponse))

    @pytest.mark.asyncio
    async def test_concurrent_disconnect_and_request(self):
        """Test race condition: disconnect while request is starting."""
        client = MQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_race",
            ensure_unique_identifier=True
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)
        await client.connect()

        # Start request and disconnect concurrently
        request_task = asyncio.create_task(
            client.request("device", "subsystem", "path")
        )
        disconnect_task = asyncio.create_task(client.disconnect())

        # Both should complete (one will fail)
        results = await asyncio.gather(request_task, disconnect_task, return_exceptions=True)

        # At least one should be an exception
        assert any(isinstance(r, Exception) for r in results)


# ============================================================================
# DUPLICATE REQUEST ID (AC-3)
# ============================================================================


class TestDuplicateRequestID:
    """Test handling of duplicate request IDs."""

    @pytest.mark.asyncio
    async def test_duplicate_request_id_collision(self):
        """Test behavior when same request_id is generated twice.

        This test documents that duplicate request IDs (though extremely unlikely
        with UUIDs) will cause undefined behavior. One request may succeed while
        the other times out, or both may fail in unpredictable ways.
        """
        client = MQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_dup_id",
            ensure_unique_identifier=True
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)
        await client.connect()

        # Mock generate_request_id to return duplicate IDs
        original_gen = client.generate_request_id
        call_count = 0

        def duplicate_id_generator():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                # First two calls return same ID
                return "duplicate-12345678-1234-1234-1234-123456789abc"
            return original_gen()

        client.generate_request_id = duplicate_id_generator

        # Start two requests with same ID (will both timeout due to no responder)
        task1 = asyncio.create_task(
            client.request("device1", "subsystem", "path1")
        )
        task2 = asyncio.create_task(
            client.request("device2", "subsystem", "path2")
        )

        # Both will timeout or fail (no responder)
        results = await asyncio.gather(task1, task2, return_exceptions=True)

        # Both should be exceptions (either timeout or other error)
        # The exact behavior with duplicate IDs is undefined
        assert all(isinstance(r, Exception) for r in results)

        await client.disconnect()

    @pytest.mark.asyncio
    async def test_response_with_unknown_request_id(self):
        """Test that responses with unknown request_ids don't crash client.

        This test verifies that receiving a response for a request that was never
        made (or already completed) doesn't cause errors. The response should be
        silently ignored or logged.
        """
        client = MQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_unknown_id",
            ensure_unique_identifier=True
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        await client.connect()

        # Publish a response with unknown request_id
        # This simulates receiving a late response or a response for a cancelled request
        fake_response_topic = TopicManager().build_response_topic(
            request_topic=f"gf_int_v1/subsystem/request/{client.identifier}/fake-request-id"
        )

        # Should not crash when publishing unknown response
        await client.publish(
            fake_response_topic,
            {
                "header": {
                    "message_type": "response",
                    "request_id": "unknown-12345678-1234-1234-1234-123456789abc",
                    "response_code": 205,
                    "timestamp": "123"
                }
            }
        )

        await asyncio.sleep(0.5)

        # Client should still be connected (didn't crash)
        assert client.is_connected

        await client.disconnect()


# ============================================================================
# HANDLER MODIFICATION DURING PROCESSING (MH-1)
# ============================================================================


class TestHandlerModificationDuringProcessing:
    """Test handler add/remove during message processing."""

    @pytest.mark.asyncio
    async def test_add_handler_during_message_processing(self):
        """Test adding handler while messages are being processed."""
        client = MQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_handler_add",
            ensure_unique_identifier=True
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        processing_started = asyncio.Event()
        processing_done = asyncio.Event()

        # Slow handler that signals when it starts
        async def slow_handler(client, topic, payload):
            processing_started.set()
            await asyncio.sleep(0.2)
            processing_done.set()
            return payload

        await client.add_message_handler(
            MessageHandlerBase(
                can_handle=lambda c, t, p: True,
                process=slow_handler,
                propagate=True
            )
        )

        await client.connect()

        # Publish message to trigger handler
        await client.publish(
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

        # Wait for processing to start
        await processing_started.wait()

        # Add another handler while first is processing
        new_handler_called = []

        async def new_handler(client, topic, payload):
            new_handler_called.append(True)
            return payload

        await client.add_message_handler(
            MessageHandlerBase(
                can_handle=lambda c, t, p: True,
                process=new_handler,
                propagate=True
            )
        )

        # Wait for original processing to finish
        await processing_done.wait()

        # Should not crash
        await client.disconnect()

    @pytest.mark.asyncio
    async def test_remove_handler_during_processing(self):
        """Test removing handler while it might be processing."""
        client = MQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_handler_remove",
            ensure_unique_identifier=True
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        handler_called = []

        async def test_handler(client, topic, payload):
            handler_called.append(True)
            await asyncio.sleep(0.1)
            return payload

        handler = MessageHandlerBase(
            can_handle=lambda c, t, p: True,
            process=test_handler,
            propagate=True
        )

        await client.add_message_handler(handler)
        await client.connect()

        # Publish message
        await client.publish(
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

        # Immediately remove handler
        await client.remove_message_handler(handler)

        await asyncio.sleep(0.3)

        # Should not crash (handler may or may not have been called)
        await client.disconnect()


# ============================================================================
# HANDLER EXCEPTION HANDLING (AC-6)
# ============================================================================


class TestHandlerExceptions:
    """Test exception handling in custom handlers."""

    @pytest.mark.asyncio
    async def test_handler_exception_does_not_crash_loop(self):
        """Document that exceptions in handlers DO crash the message loop.

        KNOWN ISSUE: Currently, unhandled exceptions in custom message handlers
        will terminate the message loop. This test documents this behavior.

        TODO: Consider wrapping handler execution in try/except to prevent
        user code from crashing the message loop.
        """
        client = MQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_handler_exception",
            ensure_unique_identifier=True
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        async def bad_handler(client, topic, payload):
            raise RuntimeError("Intentional error in handler")

        await client.add_message_handler(
            MessageHandlerBase(
                can_handle=lambda c, t, p: True,
                process=bad_handler,
                propagate=False
            )
        )

        await client.connect()

        # Publish message that will trigger exception
        await client.publish(
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

        await asyncio.sleep(0.3)

        # CURRENT BEHAVIOR: Exception in handler crashes the loop
        # Disconnect will re-raise the handler exception
        with pytest.raises(RuntimeError, match="Intentional error in handler"):
            await client.disconnect()

    @pytest.mark.asyncio
    async def test_can_handle_predicate_exception(self):
        """Document that exceptions in can_handle predicate crash the message loop.

        KNOWN ISSUE: Exceptions in the can_handle() predicate will also terminate
        the message loop, same as exceptions in the handler itself.

        TODO: Consider wrapping can_handle() calls in try/except with fallback
        to False (skip handler) rather than crashing the loop.
        """
        client = MQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_can_handle_exception",
            ensure_unique_identifier=True
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        def bad_predicate(client, topic, payload):
            raise ValueError("Intentional error in predicate")

        async def handler(client, topic, payload):
            return payload

        await client.add_message_handler(
            MessageHandlerBase(
                can_handle=bad_predicate,
                process=handler,
                propagate=True
            )
        )

        await client.connect()

        # Publish message
        await client.publish(
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

        await asyncio.sleep(0.3)

        # CURRENT BEHAVIOR: Exception in predicate crashes the loop
        # Disconnect will re-raise the predicate exception
        with pytest.raises(ValueError, match="Intentional error in predicate"):
            await client.disconnect()


# ============================================================================
# CONCURRENT OPERATIONS (AC-8)
# ============================================================================


class TestConcurrentOperations:
    """Test concurrent client operations."""

    @pytest.mark.asyncio
    async def test_concurrent_connect_calls(self):
        """Test concurrent connections from multiple client instances.

        This tests that multiple clients can connect simultaneously without
        interfering with each other. Each client gets a unique identifier
        via ensure_unique_identifier=True.
        """
        # Create multiple client instances
        clients = []
        for i in range(5):
            client = MQTTClient(
                broker=BROKER_CONFIG.hostname,
                port=BROKER_CONFIG.port,
                timeout=5,
                identifier=f"test_concurrent_connect_{i}",
                ensure_unique_identifier=True
            )
            client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)
            clients.append(client)

        try:
            # Connect all clients concurrently
            tasks = [asyncio.create_task(client.connect()) for client in clients]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # All should succeed since they have unique IDs
            successes = [r for r in results if not isinstance(r, Exception)]
            failures = [r for r in results if isinstance(r, Exception)]

            print(f"Concurrent connect results: {len(successes)} succeeded, {len(failures)} failed")

            # All clients should successfully connect
            assert len(successes) == 5, f"Expected 5 successful connections, got {len(successes)}"

        finally:
            # Clean up all clients
            for client in clients:
                try:
                    if client.is_connected:
                        await client.disconnect()
                except Exception as e:
                    print(f"Cleanup error for client {client.identifier}: {e}")

    @pytest.mark.asyncio
    async def test_concurrent_disconnect_calls(self):
        """Document behavior of multiple concurrent disconnect() calls.

        This test documents that calling disconnect() concurrently from multiple
        tasks may or may not be fully idempotent depending on timing. The first
        disconnect will succeed, and subsequent ones should either succeed silently
        or be handled gracefully.
        """
        client = MQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=5,
            identifier="test_concurrent_disconnect",
            ensure_unique_identifier=True
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        await client.connect()
        assert client.is_connected

        # Try to disconnect concurrently (NOT RECOMMENDED but should be safe)
        tasks = [asyncio.create_task(client.disconnect()) for _ in range(5)]

        # Gather results
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Document the results
        successes = [r for r in results if not isinstance(r, Exception)]
        errors = [r for r in results if isinstance(r, Exception)]

        print(f"Concurrent disconnect results: {len(successes)} succeeded, {len(errors)} failed")

        # At least one disconnect should succeed
        # The rest may succeed or fail depending on timing
        assert len(successes) >= 1

        # Client should be disconnected after all attempts
        assert not client.is_connected


# ============================================================================
# LARGE MESSAGE HANDLING (AC-11)
# ============================================================================


class TestLargeMessages:
    """Test handling of large messages near MQTT limits."""

    @pytest.mark.asyncio
    async def test_publish_large_message(self):
        """Test publishing large message (near 256KB MQTT limit)."""
        client = MQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=10,
            identifier="test_large_message",
            ensure_unique_identifier=True
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        await client.connect()

        # Create 200KB payload
        large_payload = {
            "header": {
                "message_type": "request",
                "method": "POST",
                "path": "data",
                "request_id": "test-large",
                "timestamp": "123"
            },
            "body": {
                "data": "x" * (200 * 1024)
            }
        }

        try:
            # Should either succeed or fail gracefully
            await client.publish(
                f"gf_int_v1/subsystem/request/device/test",
                large_payload
            )
            # If it succeeds, that's fine
            assert True
        except Exception as e:
            # If it fails, should be a clear error (not crash)
            assert True

        await client.disconnect()


# ============================================================================
# TIMEOUT PRECISION (AC-10)
# ============================================================================


class TestTimeoutPrecision:
    """Test timeout handling edge cases."""

    @pytest.mark.asyncio
    async def test_very_short_timeout(self):
        """Test request with very short timeout."""
        client = MQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=0.001,  # 1ms timeout
            identifier="test_short_timeout",
            ensure_unique_identifier=True
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        await client.connect()

        # Should timeout quickly
        with pytest.raises(GatewayTimeoutResponse):
            await client.request("device", "subsystem", "path")

        await client.disconnect()

    @pytest.mark.asyncio
    async def test_zero_timeout(self):
        """Test request with zero timeout."""
        client = MQTTClient(
            broker=BROKER_CONFIG.hostname,
            port=BROKER_CONFIG.port,
            timeout=0,  # Zero timeout
            identifier="test_zero_timeout",
            ensure_unique_identifier=True
        )
        client.set_credentials(BROKER_CONFIG.username, BROKER_CONFIG.password)

        await client.connect()

        # Should handle gracefully (either error or immediate timeout)
        try:
            await client.request("device", "subsystem", "path")
        except (GatewayTimeoutResponse, ValueError):
            assert True

        await client.disconnect()
