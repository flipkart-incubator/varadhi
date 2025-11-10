package com.flipkart.varadhi.web.spi;


import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import io.vertx.core.MultiMap;
import io.vertx.ext.web.RoutingContext;
import lombok.Data;

@Data
public class RequestContext {
    private URI uri;
    private MultiMap params;
    private MultiMap headers;
    private Map<String, Object> context;

    public RequestContext(RoutingContext routingContext) throws URISyntaxException {
        this.uri = new URI(routingContext.request().uri());
        this.headers = routingContext.request().headers();
        this.params = routingContext.request().params();
    }
}
