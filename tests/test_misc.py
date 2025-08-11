from httpxthrottlecache import __version__, HttpxThrottleCache, EDGAR_CACHE_RULES
import re
import pytest
import httpx 
import asyncio

import logging

logger=logging.getLogger(__name__ )
def test_version():
    assert re.match(r"\d+\.\d+\.\d+.*", __version__)


@pytest.mark.asyncio
async def test_provide_my_own(manager_nocache):
    async with httpx.AsyncClient() as myclient:
        url = "https://www.sec.gov/files/company_tickers.json"

            
        async with manager_nocache.async_http_client(client=myclient) as client:
            response = await client.get(url=url)

            first_date = response.headers["date"]
            assert response.status_code == 403, response.status_code # no header passed



@pytest.mark.asyncio
async def test_no_header(manager_cache):
    url = "https://www.sec.gov/files/company_tickers.json"

    manager_cache.httpx_params["headers"] = {}
    async with manager_cache.async_http_client() as client:
        response = await client.get(url=url)

        assert response.status_code == 403, response.status_code # no header passed


@pytest.mark.asyncio
async def test_nonedgar_cacheable(manager_cache):
    url = "https://httpbingo.org/cache/60"

    async with manager_cache.async_http_client() as client:
        response = await client.get(url=url)

        assert response.status_code == 200, response.status_code # no header passed
        date1 = response.headers["date"]

    await asyncio.sleep(1.5)
    async with manager_cache.async_http_client() as client:
        response = await client.get(url=url)

        assert response.status_code == 200, response.status_code # no header passed
        date2 = response.headers["date"]
    
    assert date1==date2

@pytest.mark.asyncio
async def test_not_cacheable(manager_cache):
    url = "https://www.sec.gov/"

    async with manager_cache.async_http_client() as client:
        response = await client.get(url=url)

        assert response.status_code == 200, response.status_code # no header passed


@pytest.mark.asyncio
async def test_short_cache_rule(manager_cache):
    url = "https://www.sec.gov/files/company_tickers.json"

    # Change cache duration to 1 second, and make sure the date is revalidated

    manager_cache.cache_rules[r".*\.sec\.gov"][r"/files/company_tickers\.json.*"] = 1
    logger.info(manager_cache.cache_rules)
    async with manager_cache.async_http_client() as client:
        response = await client.get(url=url)

        assert response.status_code == 200, response.status_code
        first_date = response.headers["date"]

    await asyncio.sleep(2)
    async with manager_cache.async_http_client() as client:
        response2 = await client.get(url=url)

        assert response2.status_code == 200, response2.status_code 

    second_date = response2.headers["date"]

    assert second_date > first_date


@pytest.mark.asyncio
async def test_explicit_params():

    mgr = HttpxThrottleCache(httpx_params={"headers": {"User-Agent": "iq de deiq@iqmo.com"}}, cache_mode="Disabled")
    url = "https://www.sec.gov/"

    async with mgr.async_http_client() as client:
        response = await client.get(url=url)

        assert response.status_code == 200, response.status_code 


@pytest.mark.asyncio
async def test_nodir():

    with pytest.raises(ValueError):
        HttpxThrottleCache(cache_dir=None)



@pytest.mark.asyncio
async def test_mkdir():
    url = "https://httpbingo.org/cache/60"

    mgr = HttpxThrottleCache(httpx_params={"headers": {}}, cache_mode=False)

    async with mgr.async_http_client() as client:
        response = await client.get(url=url)

        assert response.status_code == 200, response.status_code 


@pytest.mark.asyncio
async def test_override_cache_rule(manager_cache):
    url = "https://httpbingo.org/cache/60"

    dir = manager_cache.cache_dir


    cache_rules_zero = {"httpbingo.org": {
        ".*cache.*": 0
    }}

    mgr = HttpxThrottleCache(httpx_params={"headers": {}}, cache_dir=dir / "foo", cache_rules=cache_rules_zero)

    async with mgr.async_http_client() as client:
        response1 = await client.get(url=url)

        assert response1.status_code == 200, response1.status_code 

        await asyncio.sleep(2)
        response2 = await client.get(url=url)

        assert response2.status_code == 200, response2.status_code 

        assert response1.headers["date"] < response2.headers["date"]

    
    cache_rules_dont_cache = {"httpbingo.org": {
        ".*cache.*": False
    }}
    mgr = HttpxThrottleCache(httpx_params={"headers": {}}, cache_dir=dir / "foo", cache_rules=cache_rules_dont_cache)

    async with mgr.async_http_client() as client:
        response1 = await client.get(url=url)

        assert response1.status_code == 200, response1.status_code 

        await asyncio.sleep(2)
        response2 = await client.get(url=url)

        assert response2.status_code == 200, response2.status_code 

        assert response1.headers["date"] < response2.headers["date"]


    cache_rules_default = {"httpbingo.org": {
        ".*cache.*": None
    }}
    mgr = HttpxThrottleCache(httpx_params={"headers": {}}, cache_dir=dir / "foo", cache_rules=cache_rules_default)

    async with mgr.async_http_client() as client:
        response1 = await client.get(url=url)

        assert response1.status_code == 200, response1.status_code 

        await asyncio.sleep(2)
        response2 = await client.get(url=url)

        assert response2.status_code == 200, response2.status_code 

        assert response1.headers["date"] == response2.headers["date"]


@pytest.mark.asyncio
async def test_contextmgr():
    url = "https://httpbingo.org/cache/60"

    with HttpxThrottleCache(httpx_params={"headers": {}}, cache_mode="Disabled") as mgr:

        async with mgr.async_http_client() as client:
            response = await client.get(url=url)

            assert response.status_code == 200, response.status_code 
