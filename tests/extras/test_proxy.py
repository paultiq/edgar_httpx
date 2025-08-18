# https://github.com/monokal/docker-tinyproxy
# docker run -d --name=tinyproxy -p 6666:8888 --env FilterDefaultDeny=No  monokal/tinyproxy:latest ANY
# curl -v --proxy http://127.0.0.1:6666 http://httpbingo.org/get
import httpx
import os
from httpxthrottlecache import HttpxThrottleCache
import logging

logger=logging.getLogger(__name__)

url = "http://httpbingo.org/get"
def test_proxy_http():

    assert os.environ.get("HTTP_PROXY") is not None
    
    with httpx.Client() as client:
        response = client.get(url)

        assert response.status_code == 200
        assert "tinyproxy" in response.headers.get("via")

def test_manager_proxy(manager_nocache: HttpxThrottleCache):

    assert os.environ.get("HTTP_PROXY") is not None
    
    with manager_nocache.http_client() as client:
        response = client.get(url)

        logger.info(f"{response.headers=}")
        assert response.status_code == 200
        assert "tinyproxy" in response.headers.get("via")
