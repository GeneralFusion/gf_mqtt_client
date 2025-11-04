"""
Asyncio Event Loop Compatibility Layer for Windows.

This module provides diagnostic and configuration utilities for Python's asyncio event loop
policies, focusing on Windows compatibility across Python versions. Starting in Python 3.8,
Windows switched its default event loop policy to WindowsProactorEventLoopPolicy, which is
incompatible with some third-party libraries (such as paho-mqtt) that depend on add_reader
support.

Key Functions:
    - ensure_compatible_event_loop_policy(): Check and warn about incompatible policies
    - configure_asyncio_compatibility(): Auto-configure based on environment variables
    - set_compatible_event_loop_policy(): (Deprecated) Set Windows selector policy
    - reset_event_loop_policy(): Reset to default policy

Environment Variables:
    - ASYNCIO_COMPATIBILITY_MODE=True: Automatically use WindowsSelectorEventLoopPolicy
    - SUPPRESS_ASYNCIO_WARNINGS=True: Suppress compatibility warnings

Problem Description:
    Python 3.8+ on Windows uses WindowsProactorEventLoopPolicy by default, which doesn't
    support add_reader/add_writer methods required by paho-mqtt and similar libraries.
    This causes runtime errors like "AttributeError: 'ProactorEventLoop' has no attribute
    'add_reader'". The solution is to use WindowsSelectorEventLoopPolicy instead.

Usage:
    Call configure_asyncio_compatibility() at the start of your application before any
    asyncio operations:

    >>> from gf_mqtt_client.async_client.compatibility import configure_asyncio_compatibility
    >>> configure_asyncio_compatibility()
    >>> # Now proceed with asyncio operations

See Also:
    - https://docs.python.org/3/library/asyncio-policy.html
    - https://github.com/eclipse/paho.mqtt.python/issues/558
"""
import warnings
import os
import sys
import logging
import asyncio

logger = logging.getLogger(__name__)


def ensure_compatible_event_loop_policy() -> None:
    """
    Check and warn about potentially incompatible event loop policies on Windows.

    This diagnostic function checks if the current asyncio event loop policy is compatible
    with libraries like paho-mqtt on Windows Python 3.8+. It DOES NOT change the policy;
    it only emits warnings to help developers identify potential compatibility issues.

    The function checks:
        1. Platform: Only Windows Python 3.8+ is affected
        2. Policy: Whether WindowsSelectorEventLoopPolicy is in use
        3. Environment: Whether warnings should be suppressed

    When to Use:
        Call this early in your application to check for compatibility issues without
        automatically fixing them. Useful for diagnostics and testing.

    Environment Variables:
        SUPPRESS_ASYNCIO_WARNINGS=True: Suppresses all warnings from this function

    Raises:
        None - Only emits warnings via warnings.warn()

    Example:
        >>> from gf_mqtt_client.async_client.compatibility import ensure_compatible_event_loop_policy
        >>> ensure_compatible_event_loop_policy()
        # RuntimeWarning: Detected Windows with Python >= 3.8 using WindowsProactorEventLoopPolicy...

    Note:
        This function does NOT modify the event loop policy. To automatically fix
        compatibility issues, use configure_asyncio_compatibility() instead.
    """
    suppress_warnings = (
        os.getenv("SUPPRESS_ASYNCIO_WARNINGS", "False").lower() == "true"
    )

    if not sys.platform.startswith("win"):
        logger.debug(
            "Non-Windows platform; no event loop compatibility issues expected."
        )
        return

    if sys.version_info < (3, 8):
        logger.debug(
            "Windows + Python < 3.8 uses SelectorEventLoopPolicy by default; no action needed."
        )
        return

    current_policy = asyncio.get_event_loop_policy()
    expected_policy_cls = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)

    if expected_policy_cls is None:
        logger.debug(
            "WindowsSelectorEventLoopPolicy not available in this Python build."
        )
        return

    if isinstance(current_policy, expected_policy_cls):
        logger.debug(
            "Already using WindowsSelectorEventLoopPolicy; no warning necessary."
        )
        return

    if not suppress_warnings:
        warnings.warn(
            "Detected Windows with Python >= 3.8 using WindowsProactorEventLoopPolicy. "
            "Some libraries (e.g., paho-mqtt) may fail with errors like 'no add_reader'. "
            "Set ASYNCIO_COMPATIBILITY_MODE=True to use WindowsSelectorEventLoopPolicy or call:\n"
            "    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())\n"
            "To suppress this warning, set SUPPRESS_ASYNCIO_WARNINGS=True.",
            RuntimeWarning,
            stacklevel=2,
        )


def configure_asyncio_compatibility():
    """
    Auto-configure asyncio event loop policy for cross-platform compatibility.

    This function automatically configures the asyncio event loop policy based on the
    platform, Python version, and environment variables. It's the recommended way to
    ensure Windows compatibility with libraries like paho-mqtt.

    Configuration Logic:
        1. Non-Windows platforms: No changes (uses default policy)
        2. Windows Python < 3.8: No changes (compatible by default)
        3. Windows Python >= 3.8:
           - If ASYNCIO_COMPATIBILITY_MODE=True: Sets WindowsSelectorEventLoopPolicy
           - Otherwise: Logs informational message about potential issues

    Environment Variables:
        ASYNCIO_COMPATIBILITY_MODE=True: Enable automatic compatibility mode on Windows
        SUPPRESS_ASYNCIO_WARNINGS=True: Suppress warning messages

    When to Call:
        Call this function at the very start of your application, BEFORE any asyncio
        operations (before asyncio.run(), before creating event loops, etc.). Calling
        it after an event loop is already running will result in a warning.

    Raises:
        Does not raise - logs warnings instead if configuration fails

    Example:
        >>> import os
        >>> os.environ["ASYNCIO_COMPATIBILITY_MODE"] = "True"
        >>> from gf_mqtt_client.async_client.compatibility import configure_asyncio_compatibility
        >>> configure_asyncio_compatibility()
        # INFO: Set event loop policy to WindowsSelectorEventLoopPolicy...
        >>> # Now proceed with asyncio operations
        >>> import asyncio
        >>> asyncio.run(main())

    Note:
        If called after an event loop is already running, the function will log a
        warning and skip configuration. Always call this as early as possible in
        your application's entry point.

    See Also:
        - https://docs.python.org/3/library/asyncio-policy.html
        - https://github.com/eclipse/paho.mqtt.python/issues/558
    """
    compatibility_mode = (
        os.getenv("ASYNCIO_COMPATIBILITY_MODE", "False").lower() == "true"
    )

    # Non-Windows platforms: No configuration needed
    if not sys.platform.startswith("win"):
        logger.info(
            f"Running on {sys.platform} platform. No special event loop policy configuration needed."
        )
        return

    # Windows Python < 3.8: Already uses compatible policy
    if sys.version_info < (3, 8):
        logger.info(
            f"Running on Windows with Python {sys.version_info.major}.{sys.version_info.minor}. "
            "This version uses SelectorEventLoopPolicy by default, which is compatible with paho-mqtt."
        )
        return

    # Windows Python >= 3.8: Handle compatibility mode
    if compatibility_mode:
        try:
            # Check for a running loop to avoid RuntimeError
            try:
                loop = asyncio.get_running_loop()
                logger.warning(
                    "Cannot configure event loop policy: an event loop is already running. "
                    "Solution: Call configure_asyncio_compatibility() at the very start of your application, "
                    "BEFORE any asyncio.run(), asyncio.create_task(), or other asyncio operations.",
                    extra={"loop": str(loop), "running": loop.is_running()}
                )
                return
            except RuntimeError:  # No running loop - safe to proceed
                pass

            # Log current loop state for debugging
            current_policy = asyncio.get_event_loop_policy()
            loop = current_policy.get_event_loop()
            logger.debug(
                f"Current event loop policy: {type(current_policy).__name__}, "
                f"event loop: {loop}, running={loop.is_running()}"
            )

            # Set the compatible policy
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            logger.info(
                f"Successfully configured WindowsSelectorEventLoopPolicy for Windows Python {sys.version_info.major}.{sys.version_info.minor}. "
                "This ensures compatibility with libraries like paho-mqtt that require add_reader support. "
                "(Triggered by ASYNCIO_COMPATIBILITY_MODE=True)"
            )

            # Warn about future deprecation in Python 3.14+
            if sys.version_info >= (3, 14):
                logger.warning(
                    "Python 3.14+ detected: Event loop policy APIs may be deprecated in future versions. "
                    "Consider migrating to thread-local event loops or alternative approaches. "
                    "See https://docs.python.org/3/library/asyncio-policy.html for migration guidance."
                )

        except RuntimeError as e:
            logger.error(
                f"Failed to set event loop policy: {e}. "
                "This may indicate an event loop is already running or another configuration issue. "
                "Solution: Ensure this function is called before any asyncio operations.",
                extra={"error": str(e), "error_type": type(e).__name__}
            )
            logger.debug(
                f"Debug info - Current event loop: {loop}, running={loop.is_running()}, "
                f"policy: {type(asyncio.get_event_loop_policy()).__name__}"
            )

    else:
        # Compatibility mode not enabled - inform user
        logger.info(
            f"Running on Windows Python {sys.version_info.major}.{sys.version_info.minor} with default WindowsProactorEventLoopPolicy. "
            "ASYNCIO_COMPATIBILITY_MODE is not set. If you encounter errors like "
            "'AttributeError: ProactorEventLoop has no attribute add_reader' when using paho-mqtt or similar libraries, "
            "set environment variable ASYNCIO_COMPATIBILITY_MODE=True to enable WindowsSelectorEventLoopPolicy. "
            "To suppress this informational message, set SUPPRESS_ASYNCIO_WARNINGS=True."
        )


def set_compatible_event_loop_policy():
    """
    (Deprecated) Set WindowsSelectorEventLoopPolicy on Windows platforms.

    **This function is deprecated. Use configure_asyncio_compatibility() instead.**

    This legacy function directly sets the WindowsSelectorEventLoopPolicy on Windows
    platforms without checking environment variables or providing the full diagnostic
    capabilities of configure_asyncio_compatibility().

    Deprecation Notice:
        This function will be removed in a future version. Migrate to
        configure_asyncio_compatibility() for better control and diagnostics.

    Environment Variables:
        SUPPRESS_ASYNCIO_WARNINGS=True: Suppresses the deprecation warning

    Migration:
        >>> # Old code
        >>> set_compatible_event_loop_policy()

        >>> # New code
        >>> import os
        >>> os.environ["ASYNCIO_COMPATIBILITY_MODE"] = "True"
        >>> configure_asyncio_compatibility()

    Raises:
        Does not raise - logs warnings instead if configuration fails
    """
    suppress_warnings = (
        os.getenv("SUPPRESS_ASYNCIO_WARNINGS", "False").lower() == "true"
    )
    if not suppress_warnings:
        warnings.warn(
            "set_compatible_event_loop_policy is deprecated; use configure_asyncio_compatibility instead. "
            "Set SUPPRESS_ASYNCIO_WARNINGS=True to silence this warning.",
            DeprecationWarning,
            stacklevel=2,
        )
    if sys.platform.startswith("win"):
        try:
            # Check for a running loop to avoid RuntimeError
            try:
                loop = asyncio.get_running_loop()
                logging.warning(
                    "Cannot set event loop policy: an event loop is already running."
                )
                logger.debug(f"Active loop: {loop}")
                return
            except RuntimeError:  # No running loop
                pass

            # Log current loop state for debugging
            loop = asyncio.get_event_loop_policy().get_event_loop()
            logger.debug(f"Current event loop: {loop}, running={loop.is_running()}")

            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            logging.info(
                "Set event loop policy to WindowsSelectorEventLoopPolicy for compatibility."
            )
        except RuntimeError as e:
            logging.warning(f"Failed to set event loop policy: {e}.")
            logger.debug(f"Current event loop: {loop}, running={loop.is_running()}")
    else:
        logging.info("No special event loop policy set for non-Windows platform.")


def reset_event_loop_policy():
    """
    Reset the event loop policy to the platform default.

    Resets the asyncio event loop policy to asyncio.DefaultEventLoopPolicy, which
    restores the platform's default behavior. On Windows Python 3.8+, this means
    reverting to WindowsProactorEventLoopPolicy.

    Use Cases:
        - Testing: Reset policy between test runs
        - Cleanup: Revert temporary compatibility changes
        - Debugging: Restore default behavior for comparison

    Platform Behavior:
        - Windows Python 3.8+: Resets to WindowsProactorEventLoopPolicy
        - Windows Python < 3.8: Resets to WindowsSelectorEventLoopPolicy
        - Linux/MacOS: Resets to default Unix event loop policy

    Warning:
        Calling this function while event loops are running may cause unexpected
        behavior. Only call during application startup/shutdown or between tests.

    Example:
        >>> from gf_mqtt_client.async_client.compatibility import reset_event_loop_policy
        >>> # After testing with compatibility mode
        >>> reset_event_loop_policy()
        # INFO: Reset event loop policy to DefaultEventLoopPolicy...

    Note:
        This function does not check for running event loops. Use with caution
        in production code.
    """
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    policy_name = type(asyncio.get_event_loop_policy()).__name__
    logger.info(
        f"Reset event loop policy to DefaultEventLoopPolicy. "
        f"Current policy: {policy_name} (platform default for {sys.platform})"
    )
