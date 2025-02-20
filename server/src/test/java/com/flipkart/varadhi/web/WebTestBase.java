package com.flipkart.varadhi.web;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.varadhi.utils.JsonMapper;
import com.flipkart.varadhi.verticles.webserver.WebServerVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.vertx.core.http.HttpMethod.*;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class WebTestBase {

    protected Vertx vertx;
    protected HttpServer server;
    protected WebClient webClient;
    protected Router router;
    protected BodyHandler bodyHandler;
    protected FailureHandler failureHandler;

    protected static final int DEFAULT_PORT = 9090;
    protected static final String DEFAULT_HOST = "localhost";
    private static final long LATCH_TIMEOUT = 60L;

    public static <R, T> R jsonDeserialize(String data, Class<? extends Collection> collectionClass, Class<T> clazz) {
        try {
            JavaType valueType = JsonMapper.getMapper()
                                           .getTypeFactory()
                                           .constructCollectionType(collectionClass, clazz);
            return JsonMapper.getMapper().readValue(data, valueType);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize JSON", e);
        }
    }

    public void setUp() throws InterruptedException {
        vertx = Vertx.vertx();
        router = Router.router(vertx);
        server = vertx.createHttpServer(getHttpServerOptions());
        webClient = WebClient.create(vertx);
        bodyHandler = BodyHandler.create(false);
        failureHandler = new FailureHandler();

        CountDownLatch latch = new CountDownLatch(1);
        server.requestHandler(router).listen().onComplete(onSuccess(res -> latch.countDown()));
        awaitLatch(latch);
        bodyHandler = BodyHandler.create(false);
        failureHandler = new FailureHandler();
    }

    protected HttpServerOptions getHttpServerOptions() {
        return new HttpServerOptions().setPort(DEFAULT_PORT).setHost(DEFAULT_HOST);
    }

    public void tearDown() throws InterruptedException {
        if (webClient != null) {
            webClient.close();
        }
        if (server != null) {
            CountDownLatch latch = new CountDownLatch(1);
            server.close().onComplete(asyncResult -> {
                assertTrue(asyncResult.succeeded(), "Server close failed");
                latch.countDown();
            });
            awaitLatch(latch);
        }
    }

    public Handler<RoutingContext> wrapBlocking(Handler<RoutingContext> handler) {
        return WebServerVerticle.wrapBlockingExecution(vertx, handler);
    }

    public Route setupFailureHandler(Route route) {
        return route.failureHandler(failureHandler);
    }

    public HttpRequest<Buffer> createRequest(HttpMethod method, String path) {
        if (POST == method) {
            return webClient.post(DEFAULT_PORT, DEFAULT_HOST, path);
        } else if (GET == method) {
            return webClient.get(DEFAULT_PORT, DEFAULT_HOST, path);
        } else if (DELETE == method) {
            return webClient.delete(DEFAULT_PORT, DEFAULT_HOST, path);
        } else if (PUT == method) {
            return webClient.put(DEFAULT_PORT, DEFAULT_HOST, path);
        } else if (PATCH == method) {
            return webClient.patch(DEFAULT_PORT, DEFAULT_HOST, path);
        } else {
            throw new UnsupportedOperationException("Unsupported HTTP method");
        }
    }

    public <R> R sendRequestWithPayload(HttpRequest<Buffer> request, byte[] payload, Class<R> responseClass)
        throws InterruptedException {
        HttpResponse<Buffer> response = sendRequest(request, payload);
        assertEquals(HTTP_OK, response.statusCode(), "Unexpected status code");
        String responseBody = response.bodyAsString();
        if (responseBody == null || responseBody.isEmpty()) {
            return null;
        }
        return JsonMapper.jsonDeserialize(responseBody, responseClass);
    }

    public <R> R sendRequestWithPayload(
        HttpRequest<Buffer> request,
        byte[] payload,
        int expectedStatusCode,
        String expectedStatusMessage,
        Class<R> responseClass
    ) throws InterruptedException {
        HttpResponse<Buffer> response = sendRequest(request, payload);
        assertEquals(expectedStatusCode, response.statusCode(), "Unexpected status code");

        Optional.ofNullable(expectedStatusMessage)
                .ifPresent(
                    statusMessage -> assertEquals(statusMessage, response.statusMessage(), "Unexpected status message")
                );

        return Optional.ofNullable(responseClass)
                       .map(clazz -> JsonMapper.jsonDeserialize(response.bodyAsString(), clazz))
                       .orElse(null);
    }

    public <T, R> R sendRequestWithEntity(HttpRequest<Buffer> request, T entity, Class<R> responseClass)
        throws InterruptedException {
        return sendRequestWithPayload(request, JsonMapper.jsonSerialize(entity).getBytes(), responseClass);
    }

    public <T, R> R sendRequestWithEntity(
        HttpRequest<Buffer> request,
        T entity,
        int expectedStatusCode,
        String expectedStatusMessage,
        Class<R> responseClass
    ) throws InterruptedException {
        return sendRequestWithPayload(
            request,
            JsonMapper.jsonSerialize(entity).getBytes(),
            expectedStatusCode,
            expectedStatusMessage,
            responseClass
        );
    }

    public <R> R sendRequestWithoutPayload(HttpRequest<Buffer> request, Class<R> responseClass)
        throws InterruptedException {
        HttpResponse<Buffer> response = sendRequest(request, null);
        assertEquals(HTTP_OK, response.statusCode(), "Unexpected status code");

        return Optional.ofNullable(responseClass)
                       .map(clazz -> JsonMapper.jsonDeserialize(response.bodyAsString(), clazz))
                       .orElse(null);
    }

    public byte[] sendRequestWithoutPayload(HttpRequest<Buffer> request) throws InterruptedException {
        HttpResponse<Buffer> response = sendRequest(request, null);
        assertEquals(HTTP_OK, response.statusCode(), "Unexpected status code");
        return response.body().getBytes();
    }

    public void sendRequestWithoutPayload(
        HttpRequest<Buffer> request,
        int expectedStatusCode,
        String expectedStatusMessage
    ) throws InterruptedException {
        HttpResponse<Buffer> response = sendRequest(request, null);
        assertEquals(expectedStatusCode, response.statusCode(), "Unexpected status code");

        Optional.ofNullable(expectedStatusMessage)
                .ifPresent(
                    statusMessage -> assertEquals(statusMessage, response.statusMessage(), "Unexpected status message")
                );

        ErrorResponse error = JsonMapper.jsonDeserialize(response.body().getBytes(), ErrorResponse.class);
        assertEquals(expectedStatusMessage, error.reason());
    }


    public HttpResponse<Buffer> sendRequest(HttpRequest<Buffer> request, byte[] payload) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Future<HttpResponse<Buffer>> responseFuture = payload != null ?
            request.sendBuffer(Buffer.buffer(payload)) :
            request.send();

        PostResponseCapture<HttpResponse<Buffer>> responseCapture = new PostResponseCapture<>();
        responseFuture.onComplete(result -> {
            if (result.succeeded()) {
                responseCapture.response = result.result();
            } else {
                responseCapture.throwable = result.cause();
            }
            latch.countDown();
        });
        awaitLatch(latch);

        assertNull(responseCapture.throwable, "Request failed");
        return responseCapture.response;
    }

    public void awaitLatch(CountDownLatch latch) throws InterruptedException {
        assertTrue(latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS), "Latch await timeout");
    }

    public <T> Handler<AsyncResult<T>> onSuccess(Consumer<T> consumer) {
        return result -> {
            if (result.failed()) {
                result.cause().printStackTrace();
                fail("Async operation failed: " + result.cause().getMessage());
            } else {
                consumer.accept(result.result());
            }
        };
    }

    private static class PostResponseCapture<T> {
        private volatile T response;
        private volatile Throwable throwable;
    }
}
