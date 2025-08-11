"""An alternative cache using:
- Flat files

"""

import asyncio
import calendar
import logging
import os
import time
from pathlib import Path
from typing import Iterator, Optional, Union
from urllib.parse import quote, unquote, urlsplit

import httpx
from filelock import BaseFileLock, FileLock
from httpx._types import SyncByteStream  # protocol type

logger = logging.getLogger(__name__)


class AlreadyLockedError(Exception):
    pass


class FileCache:
    def __init__(self, cache_dir: Union[str, Path], locking: bool = True):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.locking = locking

    def to_path(self, url: str) -> Path:
        u = urlsplit(url)
        site = (u.hostname or "unknown").lower().rstrip(".")
        (self.cache_dir / site).mkdir(parents=True, exist_ok=True)
        name = unquote(u.path).strip("/").replace("/", "-") or "index"
        if u.query:
            name += "-" + unquote(u.query).replace("&", "-").replace("=", "-")
        return self.cache_dir / site / quote(name, safe="._-~")

    def store(self, url: str, content: bytes) -> bool:
        p = self.to_path(url)
        tmp = p.with_suffix(p.suffix + ".tmp")
        if self.locking:
            with FileLock(str(p) + ".lock"):
                tmp.write_bytes(content)
                os.replace(tmp, p)
                return True
        else:
            tmp.write_bytes(content)
            os.replace(tmp, p)
            return True

        # TODO / Future: Could modify the file to use the date from the response, altho at cost of extra op

    def get_if_newer(self, p: Path, validated_date: float | int) -> Path | None:
        if not p.exists():
            return None

        # if True:  # TODO: st_mtime > validated_date:
        return p
        # else:
        #    return None

    def lock(self, url: str) -> BaseFileLock:
        return FileLock(str(self.to_path(url)) + ".lock")


class _TeeCore:
    def __init__(self, resp: httpx.Response, path: Path, locking: bool, last_modified: str):
        self.resp, self.path, self.tmp = resp, path, path.with_suffix(path.suffix + ".tmp")
        self.lock = FileLock(str(path) + ".lock") if locking else None
        self.fh = None
        self.mtime = calendar.timegm(time.strptime(last_modified, "%a, %d %b %Y %H:%M:%S GMT"))

    # blocking helpers the sync/async wrappers can call (async uses anyio.to_thread.run_sync)
    def acquire(self):
        self.lock and self.lock.acquire()  # pyright: ignore[reportUnusedExpression]

    def open_tmp(self):
        self.fh = open(self.tmp, "wb")

    def write(self, chunk: bytes):
        self.fh.write(chunk)  # pyright: ignore[reportOptionalMemberAccess]

    def finalize(self):
        if self.fh:
            self.fh.flush()
            os.fsync(self.fh.fileno())
            self.fh.close()
            os.replace(self.tmp, self.path)
        self.lock and self.lock.release()  # pyright: ignore[reportUnusedExpression]
        os.utime(self.path, (self.path.stat().st_atime, self.mtime))


class _TeeToDisk(SyncByteStream):
    def __init__(self, resp: httpx.Response, path: Path, locking: bool, last_modified: str) -> None:
        self.core = _TeeCore(resp, path, locking, last_modified)

    def __iter__(self) -> Iterator[bytes]:
        self.core.acquire()
        self.core.open_tmp()
        for chunk in self.core.resp.iter_raw():
            self.core.write(chunk)
            yield chunk

    def close(self) -> None:
        try:
            self.core.resp.close()
        finally:
            self.core.finalize()


class _AsyncTeeToDisk(httpx.AsyncByteStream):
    def __init__(self, resp: httpx.Response, path: Path, locking: bool, last_modified: str) -> None:
        self.core = _TeeCore(resp, path, locking, last_modified)

    async def __aiter__(self):
        await asyncio.to_thread(self.core.acquire)
        await asyncio.to_thread(self.core.open_tmp)
        async for chunk in self.core.resp.aiter_raw():
            await asyncio.to_thread(self.core.write, chunk)
            yield chunk

    async def aclose(self) -> None:
        try:
            await self.core.resp.aclose()
        finally:
            await asyncio.to_thread(self.core.finalize)


class CachingTransport(httpx.BaseTransport, httpx.AsyncBaseTransport):
    def __init__(self, cache_dir: Union[str, Path], transport: Optional[httpx.BaseTransport] = None):
        self._cache = FileCache(cache_dir=cache_dir, locking=True)
        self.transport = transport or httpx.HTTPTransport()

    def _url_of(self, req: httpx.Request) -> str:
        s = (req.url.scheme or b"https").decode()
        h = (req.url.host or b"").decode()
        t = (req.url.target or b"").decode()
        return f"{s}://{h}{t}"

    def _cache_hit_response(self, req, path, content, ctime, mtime):
        return httpx.Response(
            status_code=200,
            headers=[
                ("content-type", "application/octet-stream"),
                ("x-cache", "HIT"),
                ("content-length", str(len(content))),
                ("Date", time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(ctime))),
                ("Last-Modified", time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(mtime))),
            ],
            content=content,
            request=req,
        )

    def _cache_miss_response(self, req, net, path, tee_factory):
        if net.status_code != 200:
            return net

        miss_headers = [
            (k, v)
            for k, v in net.headers.items()
            if k.lower() not in ("content-encoding", "content-length", "transfer-encoding")
        ]
        miss_headers.append(("x-cache", "MISS"))
        return httpx.Response(
            status_code=net.status_code,
            headers=miss_headers,
            stream=tee_factory(net, path, self._cache.locking, net.headers.get("Last-Modified")),
            request=req,
            extensions=net.extensions,
        )

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        if request.method != "GET":
            return self.transport.handle_request(request)
        path = self._cache.to_path(str(request.url))
        cached = self._cache.get_if_newer(path, validated_date=0)
        if cached is not None:
            content = path.read_bytes()
            st = path.stat()
            return self._cache_hit_response(
                request, path, content, getattr(st, "st_birthtime", st.st_ctime), st.st_mtime
            )
        net = self.transport.handle_request(request)

        if "Last-Modified" not in net.headers:
            logger.info("No Last-Modified, not caching")
            return net
        else:
            return self._cache_miss_response(request, net, path, _TeeToDisk)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.method != "GET":
            return await self.transport.handle_async_request(request)  # type: ignore[attr-defined]
        path = self._cache.to_path(str(request.url))
        cached = self._cache.get_if_newer(path, validated_date=0)
        if cached is not None:
            content = path.read_bytes()
            st = path.stat()
            return self._cache_hit_response(
                request, path, content, getattr(st, "st_birthtime", st.st_ctime), st.st_mtime
            )
        net = await self.transport.handle_async_request(request)  # type: ignore[attr-defined]

        if "Last-Modified" not in net.headers:
            logger.info("No Last-Modified, not caching")
            return net
        else:
            return self._cache_miss_response(request, net, path, _AsyncTeeToDisk)
