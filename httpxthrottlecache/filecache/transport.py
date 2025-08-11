"""An alternative cache using:
- Flat files

"""

import asyncio
import calendar
import json
import logging
import os
import time
from pathlib import Path
from typing import Iterator, Optional, Union
from urllib.parse import quote, unquote

import httpx
from filelock import BaseFileLock, FileLock
from httpx._types import SyncByteStream  # protocol type

from ..controller import get_rule_for_request

logger = logging.getLogger(__name__)


class AlreadyLockedError(Exception):
    pass


class FileCache:
    def __init__(self, cache_dir: Union[str, Path], locking: bool = True):
        self.cache_dir = Path(cache_dir)
        logger.info("cache_dir=%s", self.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.locking = locking

    def _meta_path(self, p: Path) -> Path:
        return p.with_suffix(p.suffix + ".meta")

    def _load_meta(self, p: Path) -> dict:
        try:
            return json.loads(self._meta_path(p).read_text())
        except FileNotFoundError:
            return {}

    def to_path(self, host: str, path: str, query: str) -> Path:
        site = host.lower().rstrip(".")
        (self.cache_dir / site).mkdir(parents=True, exist_ok=True)
        name = unquote(path).strip("/").replace("/", "-") or "index"
        if query:
            name += "-" + unquote(query).replace("&", "-").replace("=", "-")
        return self.cache_dir / site / quote(name, safe="._-~")

    def store(self, host: str, path: str, query: str, content: bytes, fetched_ts: float, origin_lm_ts: float) -> bool:
        p = self.to_path(host=host, path=path, query=query)
        tmp = p.with_suffix(p.suffix + ".tmp")
        if self.locking:
            with FileLock(str(p) + ".lock"):
                tmp.write_bytes(content)
                tmp.replace(p)
        else:
            tmp.write_bytes(content)
            tmp.replace(p)

        self._meta_path(p).write_text(json.dumps({"fetched": fetched_ts, "origin_lm": origin_lm_ts}))
        return True

    def get_if_fresh(
        self, host: str, path: str, query: str, cache_rules: dict[str, dict[str, Union[bool, int]]]
    ) -> tuple[bool, Path | None]:
        cached = get_rule_for_request(request_host=host, target=path, cache_rules=cache_rules)

        if not cached:
            logger.info("No cache policy for %s://%s, not retrieving from cache", host, path)
            return False, None

        p = self.to_path(host=host, path=path, query=query)
        if not p.exists():
            logger.info("Cache file doesn't exist: %s for %s", path, p)
            return False, None

        meta = self._load_meta(p)
        fetched = meta.get("fetched")
        if not fetched:
            return False, p

        if cached is True:
            logger.info("Cache policy allows unlimited cache, returning %s", p)
            return True, p

        age = time.time() - fetched
        if age < 0:
            raise ValueError(f"Age is less than 0, impossible {age=}, file {path=}")
        logger.info("file is %s seconds old, policy allows caching for up to %s", age, cached)
        return (age <= cached, p)

    def lock(self, host: str, path: str, query: str) -> BaseFileLock:
        return FileLock(str(self.to_path(host=host, path=path, query=query)) + ".lock")


class _TeeCore:
    def __init__(self, resp: httpx.Response, path: Path, locking: bool, last_modified: str, access_date: str):
        assert path is not None

        self.resp, self.path, self.tmp = resp, path, path.with_suffix(".tmp")
        self.lock = FileLock(str(path) + ".lock") if locking else None
        self.fh = None
        self.mtime = calendar.timegm(time.strptime(last_modified, "%a, %d %b %Y %H:%M:%S GMT"))
        self.atime = calendar.timegm(time.strptime(access_date, "%a, %d %b %Y %H:%M:%S GMT"))

    def acquire(self):
        self.lock and self.lock.acquire()  # pyright: ignore[reportUnusedExpression]

    def open_tmp(self):
        self.fh = open(self.tmp, "wb")

    def write(self, chunk: bytes):
        self.fh.write(chunk)  # pyright: ignore[reportOptionalMemberAccess]

    def finalize(self):
        try:
            if self.fh and not self.fh.closed:
                self.fh.flush()
                os.fsync(self.fh.fileno())
                self.fh.close()
                os.replace(self.tmp, self.path)
            try:
                meta_path = self.path.with_suffix(self.path.suffix + ".meta")
                meta_path.write_text(json.dumps({"fetched": self.atime, "origin_lm": self.mtime}))
            except FileNotFoundError:
                pass
        finally:
            if self.lock and getattr(self.lock, "is_locked", False):
                self.lock.release()


class _TeeToDisk(SyncByteStream):
    def __init__(self, resp: httpx.Response, path: Path, locking: bool, last_modified: str, access_date: str) -> None:
        self.core = _TeeCore(resp, path, locking, last_modified, access_date)

    def __iter__(self) -> Iterator[bytes]:
        self.core.acquire()
        try:
            self.core.open_tmp()
            for chunk in self.core.resp.iter_raw():
                self.core.write(chunk)
                yield chunk
        finally:
            self.core.finalize()

    def close(self) -> None:
        try:
            self.core.resp.close()
        finally:
            self.core.finalize()


class _AsyncTeeToDisk(httpx.AsyncByteStream):
    def __init__(self, resp: httpx.Response, path: Path, locking: bool, last_modified: str, access_date) -> None:
        assert path is not None
        self.core = _TeeCore(resp, path, locking, last_modified, access_date)

    async def __aiter__(self):
        await asyncio.to_thread(self.core.acquire)
        await asyncio.to_thread(self.core.open_tmp)
        try:
            async for chunk in self.core.resp.aiter_raw():
                await asyncio.to_thread(self.core.write, chunk)
                yield chunk
        finally:
            await asyncio.to_thread(self.core.finalize)

    async def aclose(self) -> None:
        try:
            await self.core.resp.aclose()
        finally:
            await asyncio.to_thread(self.core.finalize)


class CachingTransport(httpx.BaseTransport, httpx.AsyncBaseTransport):
    cache_rules: dict[str, dict[str, Union[bool, int]]]

    def __init__(
        self,
        cache_dir: Union[str, Path],
        cache_rules: dict[str, dict[str, Union[bool, int]]],
        transport: Optional[httpx.BaseTransport] = None,
    ):
        self._cache = FileCache(cache_dir=cache_dir, locking=True)
        self.transport = transport or httpx.HTTPTransport()
        self.cache_rules = cache_rules

    def _url_of(self, req: httpx.Request) -> str:
        s = (req.url.scheme or b"https").decode()
        h = (req.url.host or b"").decode()
        t = (req.url.target or b"").decode()
        return f"{s}://{h}{t}"

    def _cache_hit_response(self, req, path: Path, content, status_code: int = 200):
        meta = json.loads(path.with_suffix(path.suffix + ".meta").read_text())
        date = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(meta["fetched"]))
        last_modified = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(meta["origin_lm"]))
        return httpx.Response(
            status_code=status_code,
            headers=[
                ("content-type", "application/octet-stream"),
                ("x-cache", "HIT"),
                ("content-length", str(len(content))),
                ("Date", date),
                ("Last-Modified", last_modified),
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
            stream=tee_factory(
                net, path, self._cache.locking, net.headers.get("Last-Modified"), net.headers.get("Date")
            ),
            request=req,
            extensions=net.extensions,
        )

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        if request.method != "GET":
            return self.transport.handle_request(request)

        fresh, path = self._cache.get_if_fresh(
            request.url.host, request.url.path, request.url.query.decode(), self.cache_rules
        )
        if path:
            if fresh:
                content = path.read_bytes()
                return self._cache_hit_response(request, path, content)
            else:
                lm = json.loads(path.with_suffix(path.suffix + ".meta").read_text()).get("origin_lm")
                if lm:
                    request.headers["If-Modified-Since"] = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(lm))

        net = self.transport.handle_request(request)
        if net.status_code == 304:
            logger.info("304 for %s", request)
            assert path is not None  # must be true
            return self._cache_hit_response(request, path, path.read_bytes(), status_code=304)

        if "Last-Modified" not in net.headers:
            logger.info("No Last-Modified, not caching")
            return net
        else:
            path = self._cache.to_path(request.url.host, request.url.path, request.url.query.decode())
            return self._cache_miss_response(request, net, path, _TeeToDisk)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.method != "GET":
            return await self.transport.handle_async_request(request)  # type: ignore[attr-defined]
        fresh, path = self._cache.get_if_fresh(
            request.url.host, request.url.path, request.url.query.decode(), self.cache_rules
        )
        if path:
            if fresh:
                content = path.read_bytes()
                return self._cache_hit_response(request, path, content)
            else:
                lm = json.loads(path.with_suffix(path.suffix + ".meta").read_text()).get("origin_lm")
                if lm:
                    request.headers["If-Modified-Since"] = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(lm))

        net = await self.transport.handle_async_request(request)  # type: ignore[attr-defined]
        if net.status_code == 304:
            assert path is not None  # must be true
            logger.info("304 for %s", request)
            return self._cache_hit_response(request, path, path.read_bytes(), status_code=304)

        if "Last-Modified" not in net.headers:
            logger.info("No Last-Modified, not caching")
            return net
        else:
            path = self._cache.to_path(request.url.host, request.url.path, request.url.query.decode())
            return self._cache_miss_response(request, net, path, _AsyncTeeToDisk)
