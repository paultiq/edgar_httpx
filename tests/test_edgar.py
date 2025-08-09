import logging
import time
from pathlib import Path
from typing import Optional, Union

import httpcore
import httpx

import time
import asyncio
import pytest

from edgar_httpx import HttpClientManager


user_agent = "Iq Te teiq@iqmo.com"

SMALL_URL = "https://www.sec.gov/Archives/edgar/data/51143/000155837021009351/ibm-20210630ex32157e1d5.htm"
TEST_URL = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"

def test_file_sync(manager_cache: HttpClientManager):
    """Make two requests, make sure they succeed, and make sure the dates are the same: showing the cache hit worked"""


    with manager_cache.client() as client:
        response = client.get(url=SMALL_URL)

        first_date = response.headers["date"]
        assert response.status_code == 200, response.status_code

    
    with manager_cache.client() as client:
        response = client.get(url=SMALL_URL)

        second_date = response.headers["date"]
        assert response.status_code == 200, response.status_code

    assert first_date == second_date

    # Make 30 requests, they should complete in under a second because caching magic
    
    with manager_cache.client() as client:
        start = time.perf_counter()
        responses = [client.get(url=SMALL_URL).status_code for _ in range(30)]
        end = time.perf_counter()
    
    assert min(responses) == 200 and max(responses) == 200
    assert (end-start) < 1

def test_file_sync_nocache(manager_nocache: HttpClientManager):
    """Make two requests, make sure they succeed, and make sure the dates are the same: showing the cache hit worked"""


    with manager_nocache.client() as client:
        response = client.get(url=SMALL_URL)

        first_date = response.headers["date"]
        assert response.status_code == 200, response.status_code

    time.sleep(2)
    with manager_nocache.client() as client:
        response = client.get(url=SMALL_URL)

        second_date = response.headers["date"]
        assert response.status_code == 200, response.status_code

    assert first_date < second_date

    # Make 30 requests, they should complete in under a second because caching magic
    
    with manager_nocache.client() as client:
        start = time.perf_counter()
        responses = [client.get(url=SMALL_URL).status_code for _ in range(30)]
        end = time.perf_counter()
    
    assert min(responses) == 200 and max(responses) == 200
    assert (end-start) > 3

@pytest.mark.asyncio
async def test_file_async(manager_cache: HttpClientManager):
    """Make two requests, make sure they succeed, and make sure the dates are the same: showing the cache hit worked"""


    async with manager_cache.async_http_client() as client:
        response = await client.get(url=SMALL_URL)

        first_date = response.headers["date"]
        assert response.status_code == 200, response.status_code

    
    async with manager_cache.async_http_client() as client:
        response = await client.get(url=SMALL_URL)

        second_date = response.headers["date"]
        assert response.status_code == 200, response.status_code

    assert first_date == second_date

    # Make 30 requests, they should complete in under a second because caching magic
    
    async with manager_cache.async_http_client() as client:
        start = time.perf_counter()

        tasks = [client.get(url=SMALL_URL) for _ in range(30)]
        results = await asyncio.gather(*tasks)
        responses = [r.status_code for r in results] 
        end = time.perf_counter()
    
    assert min(responses) == 200 and max(responses) == 200
    assert (end-start) < 1


@pytest.mark.asyncio
async def test_file_async_nocache(manager_nocache: HttpClientManager):
    """Make two requests, make sure they succeed, and make sure the dates are the same: showing the cache hit worked"""


    async with manager_nocache.async_http_client() as client:
        response = await client.get(url=SMALL_URL)

        first_date = response.headers["date"]
        assert response.status_code == 200, response.status_code

    
    await asyncio.sleep(2)
    async with manager_nocache.async_http_client() as client:
        response = await client.get(url=SMALL_URL)

        second_date = response.headers["date"]
        assert response.status_code == 200, response.status_code

    assert first_date < second_date

    # Make 30 requests, they should complete in under a second because caching magic
    
    async with manager_nocache.async_http_client() as client:
        start = time.perf_counter()

        tasks = [client.get(url=SMALL_URL) for _ in range(30)]
        results = await asyncio.gather(*tasks)
        responses = [r.status_code for r in results] 
        end = time.perf_counter()
    
    assert min(responses) == 200 and max(responses) == 200
    assert (end-start) > 3, "30 requests took less than 3 seconds at 10 requests per second."

    # Change the rate limit and check again
    manager_nocache.update_rate_limiter(requests_per_second=5)
    

    async with manager_nocache.async_http_client() as client:
            
        tasks = [client.get(url=SMALL_URL) for _ in range(10)]
        results = await asyncio.gather(*tasks)
        
        start = time.perf_counter()

        tasks = [client.get(url=SMALL_URL) for _ in range(10)]
        results = await asyncio.gather(*tasks)
        responses = [r.status_code for r in results] 
        end = time.perf_counter()
        
        assert min(responses) == 200 and max(responses) == 200
        assert (end-start) > 2 and (end-start) < 3.5
    
def test_short_cache_edgar_url(manager_cache: HttpClientManager):
    url = "https://www.sec.gov/files/company_tickers.json"

    with manager_cache.client() as client:
        response = client.get(url=url)

        first_date = response.headers["date"]
        assert response.status_code == 200, response.status_code

    time.sleep(1)
    with manager_cache.client() as client:
        response = client.get(url=url)

        second_date = response.headers["date"]
        assert response.status_code == 200, response.status_code

    # Not cached
    assert first_date == second_date

def test_non_edgar_url(manager_cache: HttpClientManager):
    """Make two requests, make sure they succeed, and make sure the dates are the same: showing the cache hit worked"""

    url = "https://httpbin.org/get"
    with manager_cache.client() as client:
        response = client.get(url=url)

        first_date = response.headers["date"]
        assert response.status_code == 200, response.status_code

    time.sleep(1)
    with manager_cache.client() as client:
        response = client.get(url=url)

        second_date = response.headers["date"]
        assert response.status_code == 200, response.status_code

    # Not cached
    assert first_date < second_date


def test_close(manager_cache: HttpClientManager):

    url = "https://httpbin.org/get"
    with manager_cache.client() as client:
        response = client.get(url=url)

        first_date = response.headers["date"]
        assert response.status_code == 200, response.status_code

    manager_cache.close()

    with manager_cache.client() as client:
        response = client.get(url=url)

        first_date = response.headers["date"]
        assert response.status_code == 200, response.status_code
