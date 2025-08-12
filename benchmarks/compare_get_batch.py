import asyncio
import logging
import time

from httpxthrottlecache import HttpxThrottleCache

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

url = "https://httpbingo.org/get"
CACHE_DIR = "_cache"


def test_get_batch():
    with HttpxThrottleCache(cache_dir=CACHE_DIR, cache_mode="FileCache") as manager:
        manager.cache_rules = {"httpbingo.com": {"/file.bin": 5}}  # force stale without monkeypatching time
        manager.get_batch([url for _ in range(10)])


def test_serial_sync():
    with HttpxThrottleCache(cache_dir=CACHE_DIR, cache_mode="FileCache") as manager:
        with manager.http_client() as client:
            manager.cache_rules = {"httpbingo.com": {"/file.bin": 5}}  # force stale without monkeypatching time
            [client.get(url) for _ in range(10)]


async def test_async():
    with HttpxThrottleCache(cache_dir=CACHE_DIR, cache_mode="FileCache") as manager:
        async with manager.async_http_client() as client:
            manager.cache_rules = {"httpbingo.com": {"/file.bin": 5}}  # force stale without monkeypatching time
            await asyncio.gather(*[client.get(url) for _ in range(10)])


if __name__ == "__main__":
    s1 = time.perf_counter()
    test_get_batch()
    e1 = time.perf_counter()

    s2 = time.perf_counter()
    test_serial_sync()
    e2 = time.perf_counter()

    s3 = time.perf_counter()
    asyncio.run(test_async())
    e3 = time.perf_counter()

    logger.info("test_get_batch took %d", e1 - s1)
    logger.info("test_serial_sync took %d", e2 - s2)
    logger.info("test_async took %d", e3 - s3)
