import time


import time
import asyncio
import pytest
import os

from edgar_httpx import HttpClientManager
from concurrent.futures import ProcessPoolExecutor, wait, ThreadPoolExecutor
from pyrate_limiter import limiter_factory, Rate, Duration, MultiprocessBucket

USER_AGENT = "Iq Te teiq@iqmo.com"

SMALL_URL = "https://www.sec.gov/Archives/edgar/data/51143/000155837021009351/ibm-20210630ex32157e1d5.htm"
TEST_URL = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"


def my_task(url: str, count: int):
    assert limiter_factory.LIMITER is not None
    mgr = HttpClientManager(user_agent = USER_AGENT, rate_limiter = limiter_factory.LIMITER, cache_enabled=False)
    with mgr.client() as client:
        for _ in range(count):
            response = client.get(url)
            assert response.status_code == 200

    result = {"time": time.monotonic(), "pid": os.getpid()}
    return result


def test_threading():
    rate = Rate(10, Duration.SECOND)
    bucket = MultiprocessBucket.init([rate])
    limiter_factory.init_global_limiter(bucket)

    start = time.perf_counter()
    with ThreadPoolExecutor() as executor: 
        futures = [executor.submit(my_task, SMALL_URL, 5) for _ in range(5)]
        wait(futures)

    end = time.perf_counter()

    
    times = []
    for f in futures:
        try:
            t = f.result()
            times.append(t)
        except Exception as e:
            raise e
    assert (end-start) > 2

    
def test_multiprocessing():

    rate = Rate(10, Duration.SECOND)
    bucket = MultiprocessBucket.init([rate])

    start = time.perf_counter()
    with ProcessPoolExecutor(initializer = limiter_factory.init_global_limiter, initargs=(bucket,)) as executor: 
        futures = [executor.submit(my_task, SMALL_URL, 5) for _ in range(5)]
        wait(futures)

    end = time.perf_counter()

    
    times = []
    for f in futures:
        try:
            t = f.result()
            times.append(t)
        except Exception as e:
            raise e
    assert (end-start) > 2
