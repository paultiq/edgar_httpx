from conftest import mock_client

def test_batch(manager_cache):
    url = "https://example.com/file.bin"
    manager_cache.cache_rules = {"example.com": {"/file.bin": 5}}  # force stale without monkeypatching time

    urls = [url for _ in range(10)]

    results = manager_cache.get_batch(urls, mock_client)

    for r in results:
        assert r[0] in (200, 304)