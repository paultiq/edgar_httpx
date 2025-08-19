"""
To control rate limit across multiple processes, see https://pyratelimiter.readthedocs.io/en/latest/#backends
"""

import logging
from typing import Any

import httpx
from pyrate_limiter import Duration, InMemoryBucket, Limiter, Rate

logger = logging.getLogger(__name__)


def create_rate_limiter(requests_per_second: int, max_delay: Duration | int = Duration.DAY) -> Limiter:
    rate = Rate(requests_per_second, Duration.SECOND)
    rate_limits = [rate]

    base_bucket = InMemoryBucket(rate_limits)

    bucket = base_bucket

    limiter = Limiter(bucket, max_delay=max_delay, raise_when_fail=False, retry_until_max_delay=True)

    return limiter


class RateLimitingTransport(httpx.HTTPTransport):
    def __init__(self, limiter: Limiter, **kwargs: dict[str, Any]):
        super().__init__(**kwargs)  # pyright: ignore[reportArgumentType]
        self.limiter = limiter

    def handle_request(self, request: httpx.Request, **kwargs: dict[str, Any]) -> httpx.Response:
        # using a constant string for item name means that the same
        # rate is applied to all requests.
        if self.limiter:
            while not self.limiter.try_acquire(__name__):
                logger.debug("Lock acquisition timed out, retrying")  # pragma: no cover

            logger.debug("Acquired lock")

        logger.info("Making HTTP Request %s", request)
        return super().handle_request(request, **kwargs)


class AsyncRateLimitingTransport(httpx.AsyncHTTPTransport):
    def __init__(self, limiter: Limiter, **kwargs: dict[str, Any]):
        super().__init__(**kwargs)  # pyright: ignore[reportArgumentType]
        self.limiter = limiter

    async def handle_async_request(self, request: httpx.Request, **kwargs: dict[str, Any]) -> httpx.Response:
        if self.limiter:
            while not await self.limiter.try_acquire_async(__name__):
                logger.debug("Lock acquisition timed out, retrying")  # pragma: no cover

            logger.debug("Acquired lock")

        logger.info("Making HTTP Request %s", request)
        return await super().handle_async_request(request, **kwargs)
