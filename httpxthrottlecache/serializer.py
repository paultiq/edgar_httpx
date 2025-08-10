import json
import logging
from datetime import datetime
from typing import Tuple, Union

from hishel._serializers import (
    HEADERS_ENCODING,
    KNOWN_REQUEST_EXTENSIONS,
    KNOWN_RESPONSE_EXTENSIONS,
    BaseSerializer,
    Metadata,
    normalized_url,
)
from httpcore import Request, Response

logger = logging.getLogger(__name__)


class JSONByteSerializer(BaseSerializer):
    """JSONByteSerializer stores HTTP metadata as compact UTF-8 JSON followed by raw binary body bytes,
    separated by a single null byte. This avoids base64 encoding, significantly reducing size and
    improving performance for large responses.."""

    def dumps(self, response: Response, request: Request, metadata: Metadata) -> Union[str, bytes]:
        """
        Dumps the HTTP response and its HTTP request.
        :param response: An HTTP response
        :type response: Response
        :param request: An HTTP request
        :type request: Request
        :param metadata: Additional information about the stored response
        :type metadata: Metadata
        :return: Serialized response
        :rtype: Union[str, bytes]
        """
        response_dict = {
            "status": response.status,
            "headers": [
                (key.decode(HEADERS_ENCODING), value.decode(HEADERS_ENCODING)) for key, value in response.headers
            ],
            "extensions": {
                key: value.decode("ascii")
                for key, value in response.extensions.items()
                if key in KNOWN_RESPONSE_EXTENSIONS
            },
        }

        request_dict = {
            "method": request.method.decode("ascii"),
            "url": normalized_url(request.url),
            "headers": [
                (key.decode(HEADERS_ENCODING), value.decode(HEADERS_ENCODING)) for key, value in request.headers
            ],
            "extensions": {key: value for key, value in request.extensions.items() if key in KNOWN_REQUEST_EXTENSIONS},
        }

        metadata_dict = {
            "cache_key": metadata["cache_key"],
            "number_of_uses": metadata["number_of_uses"],
            "created_at": metadata["created_at"].strftime("%a, %d %b %Y %H:%M:%S GMT"),
        }

        full_json = {
            "response": response_dict,
            "request": request_dict,
            "metadata": metadata_dict,
        }

        return json.dumps(full_json, separators=(",", ":")).encode("utf-8") + b"\0" + response.content

    def loads(self, data: Union[str, bytes]) -> Tuple[Response, Request, Metadata]:
        """
        Loads the HTTP response and its HTTP request from serialized data.
        :param data: Serialized data
        :type data: Union[str, bytes]
        :return: HTTP response and its HTTP request
        :rtype: Tuple[Response, Request, Metadata]
        """
        data_b: bytes = data.encode("utf-8") if isinstance(data, str) else data
        full_json, body = data_b.split(b"\0", 1)
        full_json = json.loads(full_json.decode("utf-8"))
        response_dict = full_json["response"]
        request_dict = full_json["request"]
        metadata_dict = full_json["metadata"]
        metadata_dict["created_at"] = datetime.strptime(
            metadata_dict["created_at"],
            "%a, %d %b %Y %H:%M:%S GMT",
        )

        response = Response(
            status=response_dict["status"],
            headers=[
                (key.encode(HEADERS_ENCODING), value.encode(HEADERS_ENCODING))
                for key, value in response_dict["headers"]
            ],
            content=body,
            extensions={
                key: value.encode("ascii")
                for key, value in response_dict["extensions"].items()
                if key in KNOWN_RESPONSE_EXTENSIONS
            },
        )

        request = Request(
            method=request_dict["method"],
            url=request_dict["url"],
            headers=[
                (key.encode(HEADERS_ENCODING), value.encode(HEADERS_ENCODING)) for key, value in request_dict["headers"]
            ],
            extensions={
                key: value for key, value in request_dict["extensions"].items() if key in KNOWN_REQUEST_EXTENSIONS
            },
        )

        metadata = Metadata(
            cache_key=metadata_dict["cache_key"],
            created_at=metadata_dict["created_at"],
            number_of_uses=metadata_dict["number_of_uses"],
        )

        return response, request, metadata

    @property
    def is_binary(self) -> bool:  # pragma: no cover
        return True
