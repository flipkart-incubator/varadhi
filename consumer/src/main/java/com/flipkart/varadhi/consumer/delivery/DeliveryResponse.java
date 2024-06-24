package com.flipkart.varadhi.consumer.delivery;

import com.flipkart.varadhi.entities.Endpoint;

public record DeliveryResponse(int statusCode, Endpoint.Protocol protocol, byte[] body) {
}
