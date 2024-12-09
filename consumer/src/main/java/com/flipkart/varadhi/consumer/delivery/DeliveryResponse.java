package com.flipkart.varadhi.consumer.delivery;

import com.flipkart.varadhi.entities.Endpoint;

public record DeliveryResponse(int statusCode, Endpoint.Protocol protocol, byte[] responseBody) {
    public boolean success() {
        return switch (protocol) {
            case HTTP1_1, HTTP2 -> 200 <= statusCode && statusCode < 300;
            default -> throw new IllegalStateException();
        };
    }
}
