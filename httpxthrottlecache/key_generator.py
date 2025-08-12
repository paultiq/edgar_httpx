from typing import Optional

import httpcore


def file_key_generator(request: httpcore.Request, body: Optional[bytes]) -> str:
    """Generates a stable, readable key for a given request.

    Args:
        request (httpcore.Request): _description_
        body (bytes): _description_

    Returns:
        str: Persistent key for the request
    """
    host = request.url.host.decode()
    path_b, _, query_b = request.url.target.partition(b"?")
    path = path_b.decode()
    query = query_b.decode()
    url_p = path.replace("/", "__") + (f"__{query.replace('&', '__').replace('=', '__')}" if query else "")
    key = f"{host}_{url_p}"
    return key
