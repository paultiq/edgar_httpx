import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncGenerator, Callable, Generator, Iterable, Literal, Optional, Union

import hishel
import httpx
from pyrate_limiter import Duration, Limiter

from .controller import get_cache_controller
from .filecache.transport import CachingTransport
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

    httpx_params: dict[str, Any] = field(
        default_factory=lambda: {"default_encoding": "utf-8", "http2": HTTP2, "verify": True}
    )

    cache_rules: dict[str, dict[str, Union[bool, int]]] = field(default_factory=lambda: {})
    rate_limiter_enabled: bool = True
    cache_mode: Literal[False, "Disabled", "Hishel-S3", "Hishel-File", "FileCache"] = "Hishel-File"
    request_per_sec_limit: int = 10
    max_delay: Duration = field(default_factory=lambda: Duration.DAY)
    _client: Optional[httpx.Client] = None

    rate_limiter: Optional[Limiter] = None
    s3_bucket: Optional[str] = None
    s3_client: Optional[Any] = None
    user_agent: Optional[str] = None
    user_agent_factory: Optional[Callable] = None

    cache_dir: Optional[Union[Path, str]] = None

    lock = threading.Lock()

    def __post_init__(self):
        self.cache_dir = Path(self.cache_dir) if isinstance(self.cache_dir, str) else self.cache_dir
        # self.lock = threading.Lock()

        if self.rate_limiter_enabled and self.rate_limiter is None:
            self.rate_limiter = create_rate_limiter(
                requests_per_second=self.request_per_sec_limit, max_delay=self.max_delay
            )

        if (self.cache_mode != "Disabled" or self.cache_mode is False) and not self.cache_rules:
            logger.info("Cache is enabled, but no cache_rules provided. Will use default caching.")

        if self.cache_mode == "Disabled" or self.cache_mode is False:
            pass
        elif self.cache_mode == "Hishel-S3":
            if self.s3_bucket is None:
                raise ValueError("s3_bucket must be provided if using Hishel-S3 storage")
        else:  # Hishel-File or FileCache
            if self.cache_dir is None:
                raise ValueError(f"cache_dir must be provided if using a file based cache: {self.cache_mode}")
            else:
                if not self.cache_dir.exists():
                    self.cache_dir.mkdir()

    def populate_user_agent(self, params: dict):
        if self.user_agent_factory is not None:
            user_agent = self.user_agent_factory()
        else:
            user_agent = self.user_agent

        if user_agent is not None:
            if "headers" not in params:
                params["headers"] = {}

            params["headers"]["User-Agent"] = user_agent
        return params

    def get_batch(self, urls: Iterable[str], _client_mocker=None):
        """Convenience function to execute a batch of URLs from a sync context

        Current implementation uses a separate thread with an asyncio loop
        """

        async def _run():
            async def get_status_content(client, u):
                r = await client.get(u)
                if r.status_code in (200, 304):
                    return r.status_code, r.content
                else:
                    return r.status_code, None

            async with self.async_http_client() as client:
                if _client_mocker:
                    _client_mocker(client)
                return await asyncio.gather(*(get_status_content(client, u) for u in urls))

        with ThreadPoolExecutor(1) as pool:
            return pool.submit(lambda: asyncio.run(_run())).result()

    @contextmanager
    def http_client(self, bypass_cache: bool = False, **kwargs) -> Generator[httpx.Client, None, None]:
        """Provides and reuses a client. Does not close"""
        if self._client is None:
            with self.lock:
                # Locking: not super critical, since worst case might be extra httpx clients created,
                # but future proofing against TOCTOU races in free-threading world
                if self._client is None:
                    logger.info("Creating new HTTPX Client")
                    params = self.httpx_params.copy()

                    self.populate_user_agent(params)
                    params.update(**kwargs)
                    params["transport"] = self.get_transport(bypass_cache=bypass_cache)

                    self._client = httpx.Client(**params)

        yield self._client

    def close(self):
        if self._client is not None:
            self._client.close()
            self._client = None

    def update_rate_limiter(self, requests_per_second: int, max_delay: Duration = Duration.DAY):
        self.rate_limiter = create_rate_limiter(requests_per_second=requests_per_second, max_delay=Duration.DAY)

        self.close()

    def client_factory_async(self, bypass_cache: bool, **kwargs) -> httpx.AsyncClient:
        params = self.httpx_params.copy()
        params.update(**kwargs)
        self.populate_user_agent(params)
        params["transport"] = self.get_async_transport(bypass_cache=bypass_cache)

        return httpx.AsyncClient(**params)

    @asynccontextmanager
    async def async_http_client(
        self, client: Optional[httpx.AsyncClient] = None, bypass_cache: bool = False, **kwargs
    ) -> AsyncGenerator[httpx.AsyncClient, None]:
        """
        Async callers should create a single client for a group of tasks, rather than creating a single client per task.

        If a null client is passed, then this is a no-op and the client isn't closed. This (passing a client) occurs when a higher level async task creates the client to be used by child calls.
        """

        if client is not None:
            yield client  # type: ignore # Caller is responsible for closing
            return

        async with self.client_factory_async(bypass_cache=bypass_cache, **kwargs) as client:
            yield client

    def get_transport(self, bypass_cache: bool) -> httpx.BaseTransport:
        """
        Constructs the Transport Chain:

        Caching Transport (if enabled) => Rate Limiting Transport (if enabled) => httpx.HTTPTransport
        """
        if self.rate_limiter_enabled:
            assert self.rate_limiter is not None
            next_transport = RateLimitingTransport(self.rate_limiter)
        else:
            next_transport = httpx.HTTPTransport()

        if bypass_cache or self.cache_mode == "Disabled" or self.cache_mode is False:
            logger.info("Cache is DISABLED, rate limiting only")
            return next_transport
        elif self.cache_mode == "FileCache":
            assert self.cache_dir is not None
            return CachingTransport(cache_dir=self.cache_dir, transport=next_transport, cache_rules=self.cache_rules)
        else:
            # either Hishel-S3 or Hishel-File
            assert self.cache_mode == "Hishel-File" or self.cache_mode == "Hishel-S3"
            controller = get_cache_controller(key_generator=file_key_generator, cache_rules=self.cache_rules)

            if self.cache_mode == "Hishel-S3":
                assert self.s3_bucket is not None
                storage = hishel.S3Storage(
                    client=self.s3_client, bucket_name=self.s3_bucket, serializer=JSONByteSerializer()
                )
            else:
                assert self.cache_mode == "Hishel-File"
                assert self.cache_dir is not None
                storage = hishel.FileStorage(base_path=Path(self.cache_dir), serializer=JSONByteSerializer())

            return hishel.CacheTransport(transport=next_transport, storage=storage, controller=controller)

    def get_async_transport(self, bypass_cache: bool) -> httpx.AsyncBaseTransport:
        """
        Constructs the Transport Chain:

        Caching Transport (if enabled) => Rate Limiting Transport (if enabled) => httpx.HTTPTransport
        """
        if self.rate_limiter_enabled:
            assert self.rate_limiter is not None
            next_transport = AsyncRateLimitingTransport(self.rate_limiter)
        else:
            next_transport = httpx.AsyncHTTPTransport()

        if bypass_cache or self.cache_mode == "Disabled" or self.cache_mode is False:
            logger.info("Cache is DISABLED, rate limiting only")
            return next_transport
        elif self.cache_mode == "FileCache":
            assert self.cache_dir is not None
            return CachingTransport(cache_dir=self.cache_dir, transport=next_transport, cache_rules=self.cache_rules)  # pyright: ignore[reportArgumentType]
        else:
            # either Hishel-S3 or Hishel-File
            assert self.cache_mode == "Hishel-File" or self.cache_mode == "Hishel-S3"
            controller = get_cache_controller(key_generator=file_key_generator, cache_rules=self.cache_rules)

            if self.cache_mode == "Hishel-S3":
                assert self.s3_bucket is not None
                storage = hishel.AsyncS3Storage(
                    client=self.s3_client, bucket_name=self.s3_bucket, serializer=JSONByteSerializer()
                )
            else:
                assert self.cache_mode == "Hishel-File"
                assert self.cache_dir is not None
                storage = hishel.AsyncFileStorage(base_path=Path(self.cache_dir), serializer=JSONByteSerializer())

            return hishel.AsyncCacheTransport(transport=next_transport, storage=storage, controller=controller)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
