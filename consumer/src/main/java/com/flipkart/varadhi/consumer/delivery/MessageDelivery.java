package com.flipkart.varadhi.consumer.delivery;

import com.flipkart.varadhi.entities.Endpoint;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.exceptions.NotImplementedException;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.ArrayUtils;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public interface MessageDelivery {
    static MessageDelivery of(Endpoint endpoint) {
        return switch (endpoint.getProtocol()) {
            case HTTP1_1 -> new HttpMessageDelivery(endpoint);
            case HTTP2 -> throw new NotImplementedException("HTTP2 is not supported yet");
            default -> throw new IllegalArgumentException("Unsupported protocol: " + endpoint.getProtocol());
        };
    }

    CompletableFuture<DeliveryResponse> deliver(Message message)
            throws Exception;

    class HttpMessageDelivery implements MessageDelivery {
        private final Endpoint.HttpEndpoint endpoint;
        private final HttpClient httpClient;

        public HttpMessageDelivery(Endpoint endpoint) {
            this.endpoint = (Endpoint.HttpEndpoint) endpoint;
            this.httpClient = HttpClient.newBuilder()
                    .version(this.endpoint.isHttp2Supported() ? HttpClient.Version.HTTP_2 : HttpClient.Version.HTTP_1_1)
                    .connectTimeout(Duration.ofMillis(this.endpoint.getConnectTimeoutMs()))
                    .followRedirects(HttpClient.Redirect.NORMAL)
                    .build();
        }

        @Override
        public CompletableFuture<DeliveryResponse> deliver(Message message) throws Exception {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(endpoint.getUrl().toURI())
                    .timeout(Duration.ofMillis(endpoint.getRequestTimeoutMs()))
                    .header("Content-Type", endpoint.getContentType())
                    .method(
                            endpoint.getMethod(),
                            ArrayUtils.isEmpty(message.getPayload()) ? HttpRequest.BodyPublishers.noBody() :
                                    HttpRequest.BodyPublishers.ofByteArray(message.getPayload())
                    );

            // apply request headers from message
            Multimap<String, String> requestHeaders = message.getRequestHeaders();
            if (requestHeaders != null) {
                requestHeaders.entries().forEach(entry -> requestBuilder.header(entry.getKey(), entry.getValue()));
            }

            HttpRequest request = requestBuilder.build();

            return httpClient.sendAsync(
                            request, HttpResponse.BodyHandlers.ofByteArray())
                    .thenApply(response -> new DeliveryResponse(response.statusCode(), endpoint.getProtocol(),
                            response.body()
                    ))
                    .exceptionally(e -> {
                        // log error
                        return new DeliveryResponse(500, endpoint.getProtocol(), e.getMessage().getBytes());
                    });
        }
    }
}
