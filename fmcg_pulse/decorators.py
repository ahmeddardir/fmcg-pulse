"""Decorator utilities."""

import logging
import random
import time
from collections.abc import Callable
from functools import wraps

logger = logging.getLogger(__name__)


class RetriesExhaustedError(Exception):
    """Raised when a retried function exhausts all allowed attempts."""

    def __init__(self, attempts: int, last_exc: Exception) -> None:
        """Initialize the error with attempt count and last raised exception."""
        self.attempts = attempts
        self.last_exc = last_exc
        super().__init__(
            f"Retries exhausted. Total attempts: {attempts}. "
            f"Last exception: {last_exc}."
        )


def log_execution_time[**P, T](func: Callable[P, T]) -> Callable[P, T]:
    """Log the execution time of the wrapped function in milliseconds."""

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        duration_ms = (end - start) * 1000
        logger.debug("Function '%s' executed in %.2f ms", func.__name__, duration_ms)
        return result

    return wrapper


def retry_on_failure[**P, T](
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    jitter: float = 0.1,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Create a decorator that retries the wrapped function on failure.

    Uses exponential backoff with jitter between attempts.

    Args:
        max_attempts (int, optional):
            Maximum number of attempts before giving up. Defaults to 3.
        base_delay (float, optional):
            Base delay in seconds for exponential backoff. Defaults to 1.0.
        max_delay (float, optional):
            Maximum delay in seconds between attempts. Defaults to 30.0.
        jitter (float, optional):
            Upper bound for random jitter added to each delay. Defaults to 0.1.

    Raises:
        RetriesExhaustedError: If all attempts fail.

    """

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
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

            # Sanity check; loop should never exit without setting last_exc
            if last_exc is None:
                raise RuntimeError("last_exc is None after exhausting retries.")
            raise RetriesExhaustedError(max_attempts, last_exc) from last_exc

        return wrapper

    return decorator
