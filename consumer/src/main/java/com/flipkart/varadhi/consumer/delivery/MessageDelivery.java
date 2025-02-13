package com.flipkart.varadhi.consumer.delivery;

import com.flipkart.varadhi.entities.Endpoint;
import com.flipkart.varadhi.entities.Message;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.ArrayUtils;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public interface MessageDelivery {
    static MessageDelivery of(Endpoint endpoint, Supplier<HttpClient> httpClientSupplier) {
        return switch (endpoint.getProtocol()) {
            case HTTP1_1, HTTP2 -> new HttpMessageDelivery((Endpoint.HttpEndpoint)endpoint, httpClientSupplier.get());
            default -> throw new IllegalArgumentException("Unsupported protocol: " + endpoint.getProtocol());
        };
    }

    CompletableFuture<DeliveryResponse> deliver(Message message) throws Exception;

    class HttpMessageDelivery implements MessageDelivery {
        private final Endpoint.HttpEndpoint endpoint;
        private final HttpClient httpClient;

        public HttpMessageDelivery(Endpoint.HttpEndpoint endpoint, HttpClient client) {
            this.endpoint = endpoint;
            this.httpClient = client;
        }

        @Override
        public CompletableFuture<DeliveryResponse> deliver(Message message) throws Exception {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                                                            .uri(endpoint.getUri())
                                                            .timeout(Duration.ofMillis(endpoint.getRequestTimeoutMs()))
                                                            .header("Content-Type", endpoint.getContentType())
                                                            .method(
                                                                endpoint.getMethod(),
                                                                ArrayUtils.isEmpty(message.getPayload()) ?
                                                                    HttpRequest.BodyPublishers.noBody() :
                                                                    HttpRequest.BodyPublishers.ofByteArray(
                                                                        message.getPayload()
                                                                    )
                                                            );

            // apply request headers from message
            Multimap<String, String> requestHeaders = message.getHeaders();
            if (requestHeaders != null) {
                requestHeaders.entries().forEach(entry -> requestBuilder.header(entry.getKey(), entry.getValue()));
            }

            HttpRequest request = requestBuilder.build();

            // TODO: can response body handlers be pooled?
            return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray())
                             .thenApply(
                                 response -> new DeliveryResponse(
                                     response.statusCode(),
                                     endpoint.getProtocol(),
                                     response.body()
                                 )
                             );
        }
    }
}
