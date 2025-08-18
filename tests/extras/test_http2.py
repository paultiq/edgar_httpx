import pytest
from httpxthrottlecache import __version__, HttpxThrottleCache
import pytest
import aiofiles
from pathlib import Path

import logging

logger=logging.getLogger(__name__ )


@pytest.mark.asyncio
async def test_downloads_use_http2(manager_nocache: HttpxThrottleCache, tmp_path: Path):
    url = "https://www.sec.gov/"

    with manager_nocache.http_client() as client:
        r = client.get(url)
        assert r.status_code == 200
        assert r.http_version == "HTTP/2"

        out = tmp_path / "file.bin"
        r2= client.get(url)
        assert r2.http_version == "HTTP/2"
        out.write_bytes(r2.read())

        assert out.exists() and out.stat().st_size > 1024

    manager_nocache.rate_limiter_enabled = False
    async with manager_nocache.async_http_client() as client:
        r = await client.get(url)
        assert r.status_code == 200
        assert r.http_version == "HTTP/2"

        out = tmp_path / "file.bin"
        async with client.stream("GET", url) as resp:
            assert resp.http_version == "HTTP/2"
            async with aiofiles.open(out, "wb") as f:
                async for chunk in resp.aiter_bytes():
                    await f.write(chunk)
        assert out.exists() and out.stat().st_size > 1024

@pytest.mark.asyncio
async def test_downloads_use_http2_cache(manager_cache: HttpxThrottleCache, tmp_path: Path):
    url = "https://http2cdn.cdnsun.com"
    manager_cache.cache_rules[r".*"] = {r".*": 3600}

    with manager_cache.http_client() as client:
        r = client.get(url)
        assert r.status_code == 200
        assert r.http_version == "HTTP/2" 

        out1 = tmp_path / "file1.bin"
        r2= client.get(url)
        assert (r2.headers.get("x-cache") == "HIT") or (r2.extensions.get("from_cache") is True)

        out1.write_bytes(r2.read())

        assert out1.exists() and out1.stat().st_size > 0

    async with manager_cache.async_http_client() as client:
        r = await client.get(url)
        assert r.status_code == 200
        assert (r.headers.get("x-cache") == "HIT") or (r.extensions.get("from_cache") is True)

        out2 = tmp_path / "file.bin"
        async with client.stream("GET", url) as resp:
            assert resp.headers.get("x-cache") == "HIT" or resp.extensions.get("from_cache") == True
            async with aiofiles.open(out2, "wb") as f:
                async for chunk in resp.aiter_bytes():
                    await f.write(chunk)
        assert out2.exists() and out2.stat().st_size > 0
        
