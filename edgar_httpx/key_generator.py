from typing import Optional

import httpcore


def edgarfile_key_generator(request: httpcore.Request, body: Optional[bytes]) -> str:
    """Generates a stable, readable key for a given request.

    Args:
        request (httpcore.Request): _description_
        body (bytes): _description_

    Returns:
        str: Persistent key for the request
    """

    host = request.url.host.decode()
    url = request.url.target.decode()

    url_p = url.replace("/", "__")

    key = f"{host}_{url_p}"
    return key
