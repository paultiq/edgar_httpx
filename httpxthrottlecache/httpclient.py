import logging
import threading
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncGenerator, Generator, Optional

import hishel
import httpx
from pyrate_limiter import Duration, Limiter

from .controller import get_cache_controller
from .key_generator import file_key_generator
from .ratelimiter import AsyncRateLimitingTransport, RateLimitingTransport, create_rate_limiter
from .serializer import JSONByteSerializer

logger = logging.getLogger(__name__)

try:
    # enable http2 if h2 is installed
    import h2  # type: ignore  # noqa

    HTTP2 = True  # pragma: no cover
except ImportError:
    HTTP2 = False


@dataclass
class HttpxThrottleCache:
    """
    Implements a rate limited, optional-cached HTTPX wrapper that returns client() (httpx.Client) or async_http_client() (httpx.AsyncClient).

    Rate Limiting is across all connections, whether via client & async_htp_client, using pyrate_limiter. For multiprocessing, use pyrate_limiters
    MultiprocessBucket or SqliteBucket w/ a file lock.

    Caching is implemented via Hishel, which allows a variety of configurations, including AWS storage.

    This function is used for all synchronous requests.
    """

    cache_enabled: bool
    httpx_params: dict[str, Any]

    cache_rules: dict[str, dict[str, str]]
    _client: Optional[httpx.Client] = None

    def __init__(
        self,
        user_agent: Optional[str] = None,
        httpx_params: Optional[dict[str, Any]] = None,
        request_per_sec_limit: int = 10,
        max_delay: Duration = Duration.DAY,
        cache_enabled: bool = True,
        cache_dir: Optional[str] = None,
        rate_limiter: Optional[Limiter] = None,
        cache_rules: Optional[dict[str, dict[str, str]]] = None,
    ):
        self.lock = threading.Lock()

        if httpx_params is not None:
            self.httpx_params = httpx_params
        else:
            self.httpx_params = {"default_encoding": "utf-8", "http2": HTTP2, "verify": True}

        if user_agent is not None:
            if "headers" not in self.httpx_params:
                self.httpx_params["headers"] = {}

            self.httpx_params["headers"]["User-Agent"] = user_agent

        if cache_rules is not None:
            self.cache_rules = cache_rules
        else:
            self.cache_rules = {}

        if rate_limiter is None:
            self.rate_limiter = create_rate_limiter(requests_per_second=request_per_sec_limit, max_delay=max_delay)
        else:
            self.rate_limiter = rate_limiter

        self.cache_enabled = cache_enabled

        if cache_enabled:
            if not self.cache_rules:
                logger.info("Cache is enabled, but no cache_rules provided. Will use default caching.")

            if cache_dir is None:
                raise ValueError("cache_dir must be provided if cache_enabled is True")
            else:
                self.cache_dir = Path(cache_dir)
                if not self.cache_dir.exists():
                    self.cache_dir.mkdir()

    @contextmanager
    def client(self, **kwargs) -> Generator[httpx.Client, None, None]:
        """Provides and reuses a client. Does not close"""
        if self._client is None:
            with self.lock:
                # Locking: not super critical, since worst case might be extra httpx clients created,
                # but future proofing against TOCTOU races in free-threading world
                if self._client is None:
                    logger.info("Creating new HTTPX Client")
                    params = self.httpx_params.copy()
                    params.update(**kwargs)
                    params["transport"] = self.get_transport()
                    self._client = httpx.Client(**params)

        yield self._client

    def close(self):
        if self._client is not None:
            self._client.close()
            self._client = None

    def update_rate_limiter(self, requests_per_second: int, max_delay: Duration = Duration.DAY):
        self.rate_limiter = create_rate_limiter(requests_per_second=requests_per_second, max_delay=Duration.DAY)

        self.close()

    def client_factory_async(self, **kwargs) -> httpx.AsyncClient:
        params = self.httpx_params.copy()
        params.update(**kwargs)
        params["transport"] = self.get_async_transport()

        return httpx.AsyncClient(**params)

    @asynccontextmanager
    async def async_http_client(
        self, client: Optional[httpx.AsyncClient] = None, **kwargs
    ) -> AsyncGenerator[httpx.AsyncClient, None]:
        """
        Async callers should create a single client for a group of tasks, rather than creating a single client per task.

        If a null client is passed, then this is a no-op and the client isn't closed. This (passing a client) occurs when a higher level async task creates the client to be used by child calls.
        """

        if client is not None:
            yield client  # type: ignore # Caller is responsible for closing
            return

        async with self.client_factory_async(**kwargs) as client:
            yield client

    def get_transport(self) -> httpx.BaseTransport:
        if self.cache_enabled:
            logger.info("Cache is ENABLED, writing to %s", self.cache_dir)
            storage = hishel.FileStorage(base_path=self.cache_dir, serializer=JSONByteSerializer())
            controller = get_cache_controller(key_generator=file_key_generator, cache_rules=self.cache_rules)
            rate_limit_transport = RateLimitingTransport(self.rate_limiter)
            return hishel.CacheTransport(transport=rate_limit_transport, storage=storage, controller=controller)
        else:
            logger.info("Cache is DISABLED, rate limiting only")
            return RateLimitingTransport(self.rate_limiter)

    def get_async_transport(self) -> httpx.AsyncBaseTransport:
        if self.cache_enabled:
            logger.info("Cache is ENABLED, writing to %s", self.cache_dir)
            storage = hishel.AsyncFileStorage(base_path=self.cache_dir, serializer=JSONByteSerializer())
            controller = get_cache_controller(key_generator=file_key_generator, cache_rules=self.cache_rules)
            rate_limit_transport = AsyncRateLimitingTransport(self.rate_limiter)
            return hishel.AsyncCacheTransport(transport=rate_limit_transport, storage=storage, controller=controller)
        else:
            logger.info("Cache is DISABLED, rate limiting only")
            return AsyncRateLimitingTransport(self.rate_limiter)
