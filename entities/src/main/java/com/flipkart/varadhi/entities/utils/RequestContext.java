package com.flipkart.varadhi.entities.utils;

import java.net.URI;
import java.util.Map;

public record RequestContext(
    URI uri,
    Map<String, String> params,
    Map<String, String> headers,
    Map<String, Object> context
) {
}
