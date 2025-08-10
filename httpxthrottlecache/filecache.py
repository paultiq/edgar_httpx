"""An alternative cache using:
- Flat files

"""

import logging
from pathlib import Path

from filelock import FileLock

logger = logging.getLogger(__name__)


class AlreadyLockedError(Exception):
    pass


class FileCache:
    cache_dir: Path

    def __init__(self, cache_dir):
        self.cache_dir = cache_dir

    def to_path(self, url) -> Path:
        url_p = url.replace("/", "__")
        f = self.cache_dir / self.to_filename(url)

        return f

    def lock(self, url: str):
        return FileLock(self.to_path(url)).acquire(timeout=0)

    def store(self, url: str, content: bytes):
        f = self.to_path(url)

        if FileLock("my.lock").acquire(blocking=False):
            f.write_bytes(content)
            return True
        else:
            logger.info("File already locked, cache race")
            return False

        # TODO / Future: Could modify the file to use the date from the response, altho at cost of extra op

    def get_if_newer(self, url: str, validated_date):
        f = self.to_path(url)

        if f.stat().st_mtime >= validated_date:
            return f.read_bytes()
