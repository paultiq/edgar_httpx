import logging
import re
from typing import Optional, Union

import hishel
import httpcore

logger = logging.getLogger(__name__)


def get_rules(
    request_host: str, cache_rules: dict[str, dict[str, Union[bool, int]]]
) -> Optional[dict[str, Union[bool, int]]]:
    for site_pattern, rules in cache_rules.items():
        if re.match(site_pattern, request_host):
            logger.info("matched %s, using value %s: %s", site_pattern, request_host, rules)

            return rules

    logger.debug("No patterns matched %s", request_host)


def match_request(target, cache_rules_for_site: dict[str, Union[bool, int]]):
    for pat, v in cache_rules_for_site.items():
        if re.match(pat, target):
            logger.info("%s matched %s, using value %s", target, pat, v)

            return v


def get_rule_for_request(
    request_host: str, target: str, cache_rules: dict[str, dict[str, Union[bool, int]]]
) -> Optional[Union[bool, int]]:
    cache_rules_for_site = get_rules(request_host=request_host, cache_rules=cache_rules)

    if cache_rules_for_site:
        is_cacheable = match_request(target=target, cache_rules_for_site=cache_rules_for_site)
        return is_cacheable

    return None


def get_cache_controller(key_generator, cache_rules: dict[str, dict[str, Union[bool, int]]], **kwargs):
    class EdgarController(hishel.Controller):
        def is_cachable(self, request: httpcore.Request, response: httpcore.Response) -> bool:
            if response.status not in self._cacheable_status_codes:
                return False

            cache_period = get_rule_for_request(
                request_host=request.url.host.decode(), target=request.url.target.decode(), cache_rules=cache_rules
            )

            if cache_period:  # True or an Int>0
                return True
            elif cache_period is False or cache_period == 0:  # Explicitly not cacheable
                return False
            else:
                # Fall through default caching policy
                super_is_cachable = super().is_cachable(request, response)
                logger.debug("%s is cacheable %s", request.url, super_is_cachable)
                return super_is_cachable

        def construct_response_from_cache(
            self, request: httpcore.Request, response: httpcore.Response, original_request: httpcore.Request
        ) -> Union[httpcore.Request, httpcore.Response, None]:
            if (
                response.status not in self._cacheable_status_codes
            ):  # pragma: no cover - would only occur if the cache was loaded then rules changed
                return None

            cache_period = get_rule_for_request(
                request_host=request.url.host.decode(), target=request.url.target.decode(), cache_rules=cache_rules
            )

            if cache_period is True:
                # Cache forever, never recheck
                logger.debug("Cache hit for %s", request.url)
                return response
            elif (
                cache_period is False or cache_period == 0
            ):  # pragma: no cover - would only occur if the cache was loaded then rules changed
                return None
            elif cache_period:  # int
                max_age = cache_period

                age_seconds = hishel._controller.get_age(response, self._clock)

                if age_seconds > max_age:
                    logger.debug(
                        "Request needs to be validated before using %s (age=%d, max_age=%d)",
                        request.url,
                        age_seconds,
                        max_age,
                    )
                    self._make_request_conditional(request=request, response=response)
                    return request
                else:
                    logger.debug("Cache hit for %s (age=%d, max_age=%d)", request.url, age_seconds, max_age)
                    return response
            else:
                logger.debug("No rules applied to %s, using default", request.url)
                return super().construct_response_from_cache(request, response, original_request)

    controller = EdgarController(
        cacheable_methods=["GET", "POST"], cacheable_status_codes=[200], key_generator=key_generator, **kwargs
    )

    return controller
