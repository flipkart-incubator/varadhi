package com.flipkart.varadhi.consumer.delivery;

import com.flipkart.varadhi.entities.Endpoint;
import com.flipkart.varadhi.entities.ProducerMessage;
import com.flipkart.varadhi.entities.StandardHeaders;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MessageDeliveryTest {

    private static final Supplier<HttpClient> clientSupplier = () -> HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofMillis(1000))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    @Test
    void testHttpDelivery() throws Exception {
        MessageDelivery messageDelivery = MessageDelivery.of(getEndpoint("http://example.com"), clientSupplier);
        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put(StandardHeaders.MESSAGE_ID, "123");
        headers.put(StandardHeaders.GROUP_ID, "123g");
        var response = messageDelivery.deliver(new ProducerMessage(null, headers)).exceptionally(e -> {
            Assertions.fail(e);
            return null;
        }).get();
        String body = new String(response.body(), StandardCharsets.UTF_8);
        System.out.println(body);
        Assertions.assertNotNull(response);
        Assertions.assertNotNull(body);
    }

    void testHttpsDelivery() throws Exception {
        MessageDelivery messageDelivery = MessageDelivery.of(getEndpoint("https://www.reddit.com/r/Damnthatsinteresting.json"), clientSupplier);
        Multimap<String, String> headers = ArrayListMultimap.create();
        headers.put(StandardHeaders.MESSAGE_ID, "123");
        headers.put(StandardHeaders.GROUP_ID, "123g");
        var response = messageDelivery.deliver(new ProducerMessage("test".getBytes(), headers)).exceptionally(e -> {
            Assertions.fail(e);
            return null;
        }).get();
        String body = new String(response.body(), StandardCharsets.UTF_8);
        Assertions.assertNotNull(response);
        Assertions.assertNotNull(body);
    }

    @Test
    void test2() throws IOException, InterruptedException {
        HttpClient httpClient = HttpClient.newHttpClient();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://example.com"))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
    }

    private Endpoint.HttpEndpoint getEndpoint(String uri) {
        return new Endpoint.HttpEndpoint(URI.create(uri), "GET", "application/json", 5000, 5000, false);
    }

}
