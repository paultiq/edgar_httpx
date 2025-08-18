from conftest import mock_client

def test_batch(manager_cache):
    url = "https://example.com/file.bin"
    manager_cache.cache_rules = {"example.com": {"/file.bin": 5}}  # force stale without monkeypatching time

    urls = [url for _ in range(10)]

    manager_cache.get_batch(urls=urls, _client_mocker=mock_client)


def test_batch_downloads(manager_cache, tmp_path):
    url = "https://example.com/file.bin"
    manager_cache.cache_rules = {"example.com": {"/file.bin": 5}}
    url_to_path = {f"{url}?i={i}": tmp_path / f"file_{i}.bin" for i in range(10)}
    results = manager_cache.get_batch(urls=url_to_path, _client_mocker=mock_client)

    for path, expected in zip(results, url_to_path.values()):
        assert path == expected
        assert path.exists() and path.stat().st_size > 0