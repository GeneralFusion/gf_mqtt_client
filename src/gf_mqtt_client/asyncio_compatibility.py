import warnings
import os
import sys
import logging
import asyncio

logger = logging.getLogger(__name__)


def ensure_compatible_event_loop_policy() -> None:
    """
    Check whether the current event loop policy is suitable for Windows.
    If running on Windows with Python >= 3.8 and not using WindowsSelectorEventLoopPolicy,
    warn that compatibility mode may be needed. Warnings can be suppressed by setting
    the environment variable SUPPRESS_ASYNCIO_WARNINGS=True.

    Does NOT set the policy — just warns. Useful for diagnostics.
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
    Configure asyncio event loop policy based on Python version, platform, and environment variables.
    Set ASYNCIO_COMPATIBILITY_MODE=True to use WindowsSelectorEventLoopPolicy on Windows (Python >= 3.8)
    for compatibility with libraries like paho-mqtt. Set SUPPRESS_ASYNCIO_WARNINGS=True to silence warnings.
    Logs configuration status. Call early in your application.
    See https://docs.python.org/3/library/asyncio-policy.html for details on event loop policies.
    """
    compatibility_mode = (
        os.getenv("ASYNCIO_COMPATIBILITY_MODE", "False").lower() == "true"
    )

    if not sys.platform.startswith("win"):
        logging.info("No special event loop policy set for non-Windows platform.")
        return

    if sys.version_info < (3, 8):
        logging.info(
            "Python < 3.8 on Windows defaults to SelectorEventLoopPolicy; compatible by default."
        )
        return

    if compatibility_mode:
        try:
            # Check if an event loop is already created to avoid RuntimeError
            if asyncio.get_event_loop_policy().get_event_loop() is not None:
                logging.warning(
                    "Cannot set event loop policy: an event loop is already active. "
                    "Ensure configure_asyncio_compatibility() is called early in the application."
                )
                return
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            logging.info(
                "Set event loop policy to WindowsSelectorEventLoopPolicy for compatibility "
                "on Python >= 3.8 running on Windows, as requested by ASYNCIO_COMPATIBILITY_MODE=True. "
                "This is recommended for libraries like paho-mqtt."
            )
            if sys.version_info >= (3, 14):
                logging.warning(
                    "Python >= 3.14: Event loop policy APIs may be deprecated in future versions. "
                    "Consider migrating to thread-local event loops. "
                    "See https://docs.python.org/3/library/asyncio-policy.html."
                )
        except RuntimeError as e:
            logging.warning(
                f"Failed to set event loop policy: {e}. See https://docs.python.org/3/library/asyncio-policy.html "
                "for guidance on event loop configuration."
            )
    else:
        logging.info(
            "ASYNCIO_COMPATIBILITY_MODE not set for Python >= 3.8 on Windows; "
            "default WindowsProactorEventLoopPolicy will be used. If you encounter asyncio issues "
            "(e.g., AttributeError: 'ProactorEventLoop' has no attribute 'add_reader' with libraries like paho-mqtt), "
            "set ASYNCIO_COMPATIBILITY_MODE=True to use WindowsSelectorEventLoopPolicy. "
            "To suppress related warnings, set SUPPRESS_ASYNCIO_WARNINGS=True. "
            "See https://docs.python.org/3/library/asyncio-policy.html for details."
        )


def set_compatible_event_loop_policy():
    """
    Deprecated: Use configure_asyncio_compatibility instead.
    Sets the WindowsSelectorEventLoopPolicy for Windows platforms.
    Warnings can be suppressed by setting SUPPRESS_ASYNCIO_WARNINGS=True.
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
            if asyncio.get_event_loop_policy().get_event_loop() is not None:
                logging.warning(
                    "Cannot set event loop policy: an event loop is already active."
                )
                return
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            logging.info(
                "Set event loop policy to WindowsSelectorEventLoopPolicy for compatibility."
            )
        except RuntimeError as e:
            logging.warning(f"Failed to set event loop policy: {e}.")
    else:
        logging.info("No special event loop policy set for non-Windows platform.")


def reset_event_loop_policy():
    """
    Resets the event loop policy to DefaultEventLoopPolicy.
    On Windows, this restores WindowsProactorEventLoopPolicy (Python >= 3.8).
    Useful for testing or reverting compatibility changes.
    """
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    logger.info("Reset event loop policy to DefaultEventLoopPolicy.")
