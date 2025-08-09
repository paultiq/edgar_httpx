import pytest
import os
from edgar_httpx import HttpClientManager
import logging 

logger = logging.getLogger(__name__ )
logging.basicConfig(
    format='%(asctime)s %(name)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

@pytest.fixture
def manager_cache(tmp_path_factory, request):
    user_agent = os.environ["EDGAR_IDENTITY"]

    safe_name = request.node.nodeid.replace("::", "__").replace("/", "_")
    cache_dir = tmp_path_factory.mktemp(safe_name)

    mgr = HttpClientManager(user_agent=user_agent, cache_dir=cache_dir)
    return mgr

@pytest.fixture
def manager_nocache(tmp_path_factory, request):
    user_agent = os.environ["EDGAR_IDENTITY"]

    safe_name = request.node.nodeid.replace("::", "__").replace("/", "_")
    cache_dir = tmp_path_factory.mktemp(safe_name)

    mgr = HttpClientManager(user_agent=user_agent, cache_enabled=False, cache_dir=None)
    return mgr
