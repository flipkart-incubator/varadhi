package com.flipkart.varadhi.entities.utils;

import java.net.URI;
import java.util.Map;

public interface RequestContext {

    URI getURI();

    Map<String, String> getParams();

    Map<String, String> getHeaders();

    Map<String, Object> getContext();
}
