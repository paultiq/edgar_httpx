HTTPX Wrapper with Rate Limiting and Caching Transports.

[![PyPI Version](https://badge.fury.io/py/httpxthrottlecache.svg)](https://pypi.python.org/pypi/httpxthrottlecache)

[![BuildRelease](https://github.com/paultiq/httpxthrottlecache/actions/workflows/build_deploy.yml/badge.svg)](https://github.com/paultiq/httpxthrottlecache/actions/workflows/build_deploy.yml)
[![Tests](https://github.com/paultiq/httpxthrottlecache/actions/workflows/test.yml/badge.svg)](https://github.com/paultiq/httpxthrottlecache/actions/workflows/test.yml)
[![Coverage badge](https://github.com/paultiq/httpxthrottlecache/raw/python-coverage-comment-action-data/badge.svg)](https://github.com/paultiq/httpxthrottlecache/tree/python-coverage-comment-action-data)


# Introduction

The goal of this project is convenience - leveraging existing rate limiting and caching libraries with a more convenient interface and abstract away certain decisions.

This came about while implementing caching & rate limiting for [edgartools](https://edgartools.readthedocs.io/en/latest/): reducing network requests and improving overall performance led to a rabbit hole of decisions. The SEC's Edgar site has a strict 
10 request per second limit, while providing not-very-helpful caching headers. Overriding these caching headers with custom rules has a significant performance improvement.

# Caching

This project builds on [Hishel](https://hishel.com/) to provide: 

- Easily configurable File Storage and (TBD) AWS S3 Storage
- Cache Controller driven by rules, defined as:
    ```py
    {
        'site_regex': {
            'url_regex': duration
        }
    }
    ```

    duration is int | bool:
    - int: # of seconds to treat the file as unchanged: the file will not be revalidated during this period
    - true: infinite caching - never re-validate
    -  
- Rate Limiting supporting local rate limits, multiprocessing rate limiting, and distributed rate limiting. 

