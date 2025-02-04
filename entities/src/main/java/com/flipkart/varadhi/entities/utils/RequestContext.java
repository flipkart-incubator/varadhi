package com.flipkart.varadhi.entities.utils;

import lombok.Data;

import java.net.URI;
import java.util.Map;

@Data
public class RequestContext {
    private URI getURI;
    private Map<String, String> params;
    private Map<String, String> headers;
    private Map<String, Object> gontext;
}
