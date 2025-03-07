package com.flipkart.varadhi.spi;


import java.net.URI;
import java.util.Map;
import io.vertx.core.MultiMap;
import lombok.Data;

@Data
public class RequestContext {
    private URI uri;
    private MultiMap params;
    private MultiMap headers;
    private Map<String, Object> context;

}
