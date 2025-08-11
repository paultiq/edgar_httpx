HTTPX Wrapper with Rate Limiting and Caching Transports.

[![PyPI Version](https://badge.fury.io/py/httpxthrottlecache.svg)](https://pypi.python.org/pypi/httpxthrottlecache)
![Python Versions](https://img.shields.io/pypi/pyversions/httpxthrottlecache)

[![BuildRelease](https://github.com/paultiq/httpxthrottlecache/actions/workflows/build_deploy.yml/badge.svg)](https://github.com/paultiq/httpxthrottlecache/actions/workflows/build_deploy.yml)
[![Tests](https://github.com/paultiq/httpxthrottlecache/actions/workflows/test.yml/badge.svg)](https://github.com/paultiq/httpxthrottlecache/actions/workflows/test.yml)
[![Coverage badge](https://github.com/paultiq/httpxthrottlecache/raw/python-coverage-comment-action-data/badge.svg)](https://github.com/paultiq/httpxthrottlecache/tree/python-coverage-comment-action-data)


# Introduction

The goal of this project is a combination of convenience and as a demonstration of how to assemble HTTPX Transports in different combinations. 

HTTP request libraries, rate limiting and caching are topics with deep rabbit holes: the technical implementation details & the decisions an end user has to make. The convenience part of this package is abstracting away certain decisions and making certain opinionated decisions as to how caching & rate limiting should be controlled. 

This came about while implementing caching & rate limiting for [edgartools](https://edgartools.readthedocs.io/en/latest/): reducing network requests and improving overall performance led to a myriad of decisions. The SEC's Edgar site has a strict 
10 request per second limit, while providing not-very-helpful caching headers. Overriding these caching headers with custom rules is necessary in certain cases. 

# Caching

This project provides four `cache_mode` options:
- Disabled: Rate Limiting only
- Hishel-File: Cache using Hishel using FileStorage
- Hishel-S3: Cache using Hishel using S3Storage
- FileCache: Use a simpler filecache backend that uses file modified and created time and only revalidates using last-modified. For sites where last-modified is provided. 

Cache Rules are defined as a dictionary of site regular expressions to path regular expressions. 
```py
{
    'site_regex': {
        'url_regex': duration,
        'url_regex2': duration,
        '.*': 3600, # cache all paths for this site for an hour
    }
}
```

## FileCache

The FileCache implementation stores files with a .meta sidecar. The .meta contains any additional file metadata that will increment on revalidation. 

FileCache currently only revalidates using Last-Modified. Will do Etag support when I have a need for it (PRs welcome).

# Rate Limiting

Rate limiting is implemented via [pyrate_limiter](https://pyratelimiter.readthedocs.io/en/latest/). This is a leaky bucket implementation that allows a configurable number of requests per time interval. 

pyrate_limiter supports a variable of backends. The default backend is in-memory, and a single Limiter can be used for both sync and asyncio requests, across multiple threads. Alternative limiters can be used for multiprocess and distributed rate limiting, see [examples](https://github.com/vutran1710/PyrateLimiter/tree/master/examples) for more. 