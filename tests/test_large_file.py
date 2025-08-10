from edgar_httpx import HttpClientManager
import time

LARGE_FILE_URL = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
 
def test_large_file(manager_cache: HttpClientManager):
    with manager_cache.client() as client:
        start = time.perf_counter()
        response = client.get(LARGE_FILE_URL)
        end = time.perf_counter()

        assert response.status_code == 200
        first_duration = end - start

        assert 5 < first_duration < 120 # this is a rough estimate to make sure the file took about as long as it should

        start = time.perf_counter()
        response = client.get(LARGE_FILE_URL)
        end = time.perf_counter()

        assert response.status_code == 200
        second_duration = end - start

        assert second_duration < 5 # this is a rough estimate

    