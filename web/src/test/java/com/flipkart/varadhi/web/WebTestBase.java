package com.flipkart.varadhi.web;

import com.fasterxml.jackson.core.type.TypeReference;
import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.SpanProvider;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.web.ErrorResponse;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
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
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.vertx.core.http.HttpMethod.*;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;

public class WebTestBase {

    protected Vertx vertx;
    protected HttpServer server;
    protected WebClient webClient;
    protected Router router;
    protected BodyHandler bodyHandler;
    protected FailureHandler failureHandler;
    protected ResourceReadCache<Resource.EntityResource<Project>> projectCache;
    protected ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache;
    protected InMemorySpanExporter spanExporter;
    protected SdkTracerProvider tracerProvider;
    protected OpenTelemetrySdk openTelemetry;
    protected Tracer tracer;
    protected SpanProvider spanProvider;

    protected static final int DEFAULT_PORT = 9090;
    protected static final String DEFAULT_HOST = "localhost";
    private static final long LATCH_TIMEOUT = 60L;

    public void setUp() throws InterruptedException {
        projectCache = Mockito.mock(ResourceReadCache.class);
        topicCache = Mockito.mock(ResourceReadCache.class);

        vertx = Vertx.vertx();
        router = Router.router(vertx);
        server = vertx.createHttpServer(getHttpServerOptions());
        webClient = WebClient.create(vertx);
        bodyHandler = BodyHandler.create(false);
        failureHandler = new FailureHandler();

        CountDownLatch latch = new CountDownLatch(1);
        server.requestHandler(router).listen().onComplete(onSuccess(res -> latch.countDown()));
        awaitLatch(latch);
        setupTracer();
    }

    @BeforeAll
    public static void setupInitialConfig() {
        if (!StdHeaders.isGlobalInstanceInitialized()) {
            StdHeaders.init(TestStdHeaders.get());
        }
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
                Assertions.assertTrue(asyncResult.succeeded(), "Server close failed");
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

    public void setupTracer() {
        spanExporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spanExporter)).build();

        openTelemetry = OpenTelemetrySdk.builder().setTracerProvider(tracerProvider).build();
        tracer = openTelemetry.getTracer("testTracer");
        spanProvider = new SpanProvider(tracer);
    }

    // ~~~ Refactored Request Sending Methods ~~~

    public <R> R sendRequestWithPayload(HttpRequest<Buffer> request, byte[] payload, ClassRef<R> responseClassRef) {
        int expectedStatusCode = (responseClassRef == null) ? HTTP_NO_CONTENT : HTTP_OK;
        return sendRequestAndParseResponse(request, payload, expectedStatusCode, null, responseClassRef);
    }

    public <T, R> R sendRequestWithEntity(HttpRequest<Buffer> request, T entity, ClassRef<R> responseClassRef) {
        return sendRequestWithPayload(request, JsonMapper.jsonSerialize(entity).getBytes(), responseClassRef);
    }

    public <T, R> R sendRequestWithEntity(
        HttpRequest<Buffer> request,
        T entity,
        int expectedStatusCode,
        String expectedStatusMessage,
        ClassRef<R> responseClassRef
    ) {
        return sendRequestAndParseResponse(
            request,
            JsonMapper.jsonSerialize(entity).getBytes(),
            expectedStatusCode,
            expectedStatusMessage,
            responseClassRef
        );
    }

    public <R> R sendRequestWithoutPayload(HttpRequest<Buffer> request, ClassRef<R> responseClassRef) {
        return sendRequestWithPayload(request, null, responseClassRef);
    }

    public byte[] sendRequestWithoutPayload(HttpRequest<Buffer> request) {
        return sendRequestWithoutPayload(request, HTTP_OK);
    }

    @SneakyThrows
    public byte[] sendRequestWithoutPayload(HttpRequest<Buffer> request, int expectedStatusCode) {
        HttpResponse<Buffer> response = executeRequest(request, null);
        Assertions.assertEquals(
            expectedStatusCode,
            response.statusCode(),
            () -> "Unexpected status code. response is : " + response.bodyAsString()
        );
        if (response.body() == null) {
            return null;
        }
        return response.body().getBytes();
    }

    public void sendRequestWithoutPayload(
        HttpRequest<Buffer> request,
        int expectedStatusCode,
        String expectedStatusMessage
    ) throws InterruptedException {
        HttpResponse<Buffer> response = executeRequest(request, null);
        Assertions.assertEquals(
            expectedStatusCode,
            response.statusCode(),
            () -> "Unexpected status code. response is : " + response.bodyAsString()
        );

        Optional.ofNullable(expectedStatusMessage)
                .ifPresent(
                    statusMessage -> Assertions.assertEquals(
                        statusMessage,
                        response.statusMessage(),
                        "Unexpected status message"
                    )
                );

        ErrorResponse error = JsonMapper.jsonDeserialize(response.body().getBytes(), ErrorResponse.class);
        Assertions.assertEquals(expectedStatusMessage, error.reason());
    }


    // ~~~ Private Helper Methods ~~~

    /**
     * The new base method that executes a request, validates the response, and parses the body.
     */

    @SneakyThrows
    public <R> R sendRequestAndParseResponse(
        HttpRequest<Buffer> request,
        byte[] payload,
        int expectedStatusCode,
        String expectedStatusMessage,
        ClassRef<R> responseClassRef
    ) {
        HttpResponse<Buffer> response = executeRequest(request, payload);

        Assertions.assertEquals(
            expectedStatusCode,
            response.statusCode(),
            () -> String.format(
                "Unexpected status code. Expected %d, but got %d. Response is: %s",
                expectedStatusCode,
                response.statusCode(),
                response.bodyAsString()
            )
        );

        Optional.ofNullable(expectedStatusMessage)
                .ifPresent(
                    statusMessage -> Assertions.assertEquals(
                        statusMessage,
                        response.statusMessage(),
                        "Unexpected status message"
                    )
                );

        if (responseClassRef == null) {
            return null;
        }

        String responseBody = response.bodyAsString();
        if (responseBody == null || responseBody.isEmpty()) {
            return null;
        }

        if (responseClassRef instanceof T<R> typeRef) {
            return JsonMapper.getMapper().readValue(responseBody, typeRef.ref);
        } else if (responseClassRef instanceof C<R> classRef) {
            return JsonMapper.getMapper().readValue(responseBody, classRef.clazz);
        }
        throw new IllegalArgumentException("Unsupported ClassRef type: " + responseClassRef.getClass().getName());
    }

    /**
     * Executes the web request and waits for the response.
     */
    public HttpResponse<Buffer> executeRequest(HttpRequest<Buffer> request, byte[] payload)
        throws InterruptedException {
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

        Assertions.assertNull(responseCapture.throwable, "Request failed");
        return responseCapture.response;
    }

    public void awaitLatch(CountDownLatch latch) throws InterruptedException {
        Assertions.assertTrue(latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS), "Latch await timeout");
    }

    public <T> Handler<AsyncResult<T>> onSuccess(Consumer<T> consumer) {
        return result -> {
            if (result.failed()) {
                result.cause().printStackTrace();
                Assertions.fail("Async operation failed: " + result.cause().getMessage());
            } else {
                consumer.accept(result.result());
            }
        };
    }

    private static class PostResponseCapture<T> {
        private volatile T response;
        private volatile Throwable throwable;
    }

    // method that takes object, serialized to string and then reads it back using jackson's type reference object


    public static sealed class ClassRef<Type> permits T, C {
    }


    @AllArgsConstructor
    public static final class T<T> extends ClassRef<T> {
        private final TypeReference<T> ref;
    }


    @AllArgsConstructor
    public static final class C<T> extends ClassRef<T> {
        private final Class<T> clazz;
    }

    public static <T> ClassRef<T> t(TypeReference<T> ref) {
        return new WebTestBase.T<>(ref);
    }

    public static <T> ClassRef<T> c(Class<T> clazz) {
        return new WebTestBase.C<>(clazz);
    }
}
