package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.net.URL;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "@endpointType"
)
@JsonSubTypes({
        @JsonSubTypes.Type(Endpoint.HttpEndpoint.class),
})
public abstract sealed class Endpoint {

    abstract Protocol getProtocol();

    public enum Protocol {
        HTTP1_1,
        HTTP2,
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    public static final class HttpEndpoint extends Endpoint {
        private final URL url;
        private final String method;
        private final String contentType;
        private final long connectTimeoutMs;
        private final long requestTimeoutMs;

        private final boolean http2Supported;

        @Override
        Protocol getProtocol() {
            return http2Supported ? Protocol.HTTP2 : Protocol.HTTP1_1;
        }
    }
}
