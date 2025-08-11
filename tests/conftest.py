import pytest
import os
from httpxthrottlecache import HttpxThrottleCache, EDGAR_CACHE_RULES
import logging 

logger = logging.getLogger(__name__ )
logging.basicConfig(
    format='%(asctime)s %(name)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

@pytest.fixture(params=["Hishel-File", "FileCache"], ids=["hishel", "filecache"])
def manager_cache(tmp_path_factory, request):
    user_agent = os.environ["EDGAR_IDENTITY"]
    cache_dir = tmp_path_factory.mktemp("cache")
    return HttpxThrottleCache(user_agent=user_agent, cache_dir=cache_dir, cache_mode=request.param, cache_rules=EDGAR_CACHE_RULES)

@pytest.fixture
def manager_nocache():
    user_agent = os.environ["EDGAR_IDENTITY"]
    mgr = HttpxThrottleCache(user_agent=user_agent, cache_mode="Disabled", cache_dir=None)
    return mgr
