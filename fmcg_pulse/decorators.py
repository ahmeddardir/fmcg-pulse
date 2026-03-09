"""Decorator utilities."""

import logging
import random
import time
from functools import wraps

logger = logging.getLogger(__name__)


class RetriesExhaustedError(Exception):
    """Raised when a retried function exhausts all allowed attempts."""

    def __init__(self, attempts, last_exc) -> None:
        self.attempts = attempts
        self.last_exc = last_exc
        super().__init__(
            f"Retries exhausted. Total attempts: {attempts}. "
            f"Last exception: {last_exc}."
        )


def log_execution_time(func):
    """Decorator that logs the execution time of the wrapped function in ms."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        duration_ms = (end - start) * 1000
        logger.debug("Function '%s' executed in %.2f ms", func.__name__, duration_ms)
        return result

    return wrapper


def retry_on_failure(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    jitter: float = 0.1,
):
    """
    Decorator factory that retries the wrapped function on failure.

    Uses exponential backoff with jitter between attempts.
    Raises RetriesExhaustedError if all attempts fail.

    Args:
        max_attempts: Maximum number of attempts before giving up.
        base_delay: Base delay in seconds for exponential backoff.
        max_delay: Maximum delay in seconds between attempts.
        jitter: Upper bound for random jitter added to each delay.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    logger.warning(
                        "'%s' %d/%d failed: %s", func.__name__, attempt, max_attempts, e
                    )
                    # Avoid sleeping on the final attempt
                    if attempt < max_attempts:
                        time.sleep(
                            min(
                                max_delay,
                                base_delay * (2**attempt) + random.uniform(0, jitter),
                            )
                        )
            logger.error("'%s' failed after %d attempts.", func.__name__, max_attempts)
            raise RetriesExhaustedError(max_attempts, last_exc) from last_exc

        return wrapper

    return decorator
