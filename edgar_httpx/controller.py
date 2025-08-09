import logging
import re
from typing import Union

import hishel
import httpcore

logger = logging.getLogger(__name__)


def match_request(target, cache_rules: dict[str, Union[bool, int]]):
    for pat, v in cache_rules.items():
        if re.match(pat, target):
            logger.info("%s matched %s, using value %s", target, pat, v)

            return v


def get_cache_controller(key_generator, cache_rules: dict[str, Union[bool, int]], **kwargs):
    class EdgarController(hishel.Controller):
        def is_cachable(self, request: httpcore.Request, response: httpcore.Response) -> bool:
            if response.status not in self._cacheable_status_codes:
                return False

            if request.url.host.decode().endswith("sec.gov"):
                target = request.url.target.decode()

                is_cacheable = match_request(target=target, cache_rules=cache_rules)
                if is_cacheable:
                    return True
                else:
                    return False
            else:
                super_is_cachable = super().is_cachable(request, response)
                logger.debug("%s is cacheable %s", request.url, super_is_cachable)
                return super_is_cachable

        def construct_response_from_cache(
            self, request: httpcore.Request, response: httpcore.Response, original_request: httpcore.Request
        ) -> Union[httpcore.Request, httpcore.Response, None]:
            if response.status not in self._cacheable_status_codes:
                return None

            target = request.url.target.decode()

            if request.url.host.decode().endswith("sec.gov"):
                cache_period = match_request(target=target, cache_rules=cache_rules)

                if cache_period is True:
                    # Cache forever, never recheck
                    logger.debug("Cache hit for %s", target)
                    return response
                elif cache_period is False:
                    # Do not cache
                    return None
                else:
                    max_age = cache_period

                    age_seconds = hishel._controller.get_age(response, self._clock)

                    if age_seconds > max_age:
                        logger.debug(
                            "Request needs to be validated before using %s (age=%d, max_age=%d)",
                            target,
                            age_seconds,
                            max_age,
                        )
                        self._make_request_conditional(request=request, response=response)
                        return request
                    else:
                        logger.debug("Cache hit for %s (age=%d, max_age=%d)", target, age_seconds, max_age)
                        return response
            else:
                return super().construct_response_from_cache(request, response, original_request)

    controller = EdgarController(
        cacheable_methods=["GET", "POST"], cacheable_status_codes=[200], key_generator=key_generator, **kwargs
    )

    return controller
