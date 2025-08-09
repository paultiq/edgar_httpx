from edgar_httpx import __version__, HttpClientManager
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
    url = "https://httpbin.org/cache/60"

    async with manager_cache.async_http_client() as client:
        response = await client.get(url=url)

        assert response.status_code == 200, response.status_code # no header passed
        date1 = response.headers["date"]

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
    manager_cache.cache_rules[r"/files/company_tickers\.json.*"] = 1
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

    mgr = HttpClientManager(httpx_params={"headers": {"User-Agent": "iq de deiq@iqmo.com"}}, cache_enabled=False)
    url = "https://www.sec.gov/"

    async with mgr.async_http_client() as client:
        response = await client.get(url=url)

        assert response.status_code == 200, response.status_code 


@pytest.mark.asyncio
async def test_nodir():

    with pytest.raises(ValueError):
        mgr = HttpClientManager(cache_enabled=True, cache_dir=None)



@pytest.mark.asyncio
async def test_mkdir(manager_cache):
    url = "https://httpbin.org/cache/60"

    dir = manager_cache.cache_dir

    mgr = HttpClientManager(httpx_params={"headers": {"User-Agent": "iq de deiq@iqmo.com"}}, cache_enabled=True, cache_dir=dir / "foo")

    async with mgr.async_http_client() as client:
        response = await client.get(url=url)

        assert response.status_code == 200, response.status_code 

