from httpxthrottlecache import HttpxThrottleCache
import time
import logging
logger = logging.getLogger(__name__)
#
# This is too large... expensive test for a GHA RunnerLARGE_FILE_URL = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
 
FILE_URL = "https://www.sec.gov/Archives/edgar/data/51143/000155837021009351/ibm-20210630ex32157e1d5.htm"

def test_large_file(manager_cache: HttpxThrottleCache):
    with manager_cache.http_client() as client:

        logger.info("Making first request")
        start = time.perf_counter()
        r1 = client.get(FILE_URL)
        assert r1.headers.get("x-cache") == "MISS" or r1.extensions.get("from_cache") == False

        end = time.perf_counter()

        assert r1.status_code == 200
        first_duration = end - start

        assert 0 < first_duration < 120 # this is a rough estimate to make sure the file took about as long as it should

        logger.info("Making second request")
        start = time.perf_counter()
        r2 = client.get(FILE_URL)
        assert r2.headers.get("x-cache") == "HIT" or r2.extensions.get("from_cache") == True

        end = time.perf_counter()

        assert r2.status_code == 200
        second_duration = end - start

        assert second_duration < 20 # generous time
    