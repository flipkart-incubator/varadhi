package com.flipkart.varadhi.web;

import com.fasterxml.jackson.databind.JavaType;
import com.flipkart.varadhi.RestVerticle;
import com.flipkart.varadhi.utils.JsonMapper;
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
import org.junit.jupiter.api.Assertions;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.vertx.core.http.HttpMethod.*;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WebTestBase {

    protected Vertx vertx;
    protected HttpServer server;
    protected WebClient webClient;
    protected Router router;
    protected BodyHandler bodyHandler;
    protected FailureHandler failureHandler;
    protected int defaultPort = 9090; //use port different from default 8080, conflicts with server port in e2e.
    protected String defaultHost = "localhost";

    public static <R, T> R jsonDeserialize(String data, Class<? extends Collection> collectionClass, Class<T> clazz)
            throws Exception {
        JavaType type = JsonMapper.getMapper().getTypeFactory().constructCollectionType(collectionClass, clazz);
        return (R) JsonMapper.getMapper().readValue(data, type);

    }

    public void setUp() throws InterruptedException {
        vertx = Vertx.vertx();
        router = Router.router(vertx);
        server = vertx.createHttpServer(getHttpServerOptions());
        webClient = WebClient.create(vertx);
        CountDownLatch latch = new CountDownLatch(1);
        server.requestHandler(router).listen().onComplete(onSuccess(res -> latch.countDown()));
        awaitLatch(latch);
        bodyHandler = BodyHandler.create(false);
        failureHandler = new FailureHandler();
    }

    protected HttpServerOptions getHttpServerOptions() {
        return new HttpServerOptions().setPort(defaultPort).setHost(defaultHost);
    }

    public void tearDown() throws InterruptedException {
        if (webClient != null) {
            webClient.close();
        }
        if (server != null) {
            CountDownLatch latch = new CountDownLatch(1);
            server.close().onComplete((asyncResult) -> {
                assertTrue(asyncResult.succeeded());
                latch.countDown();
            });
            awaitLatch(latch);
        }
    }

    protected Handler<RoutingContext> wrapBlocking(Handler<RoutingContext> handler) {
        return RestVerticle.wrapBlockingExecution(vertx, handler);
    }

    protected Route setupFailureHandler(Route route) {
        return route.failureHandler(failureHandler);
    }

    protected HttpRequest<Buffer> createRequest(HttpMethod method, String path) {
        if (POST == method) {
            return webClient.post(defaultPort, defaultHost, path);
        } else if (GET == method) {
            return webClient.get(defaultPort, defaultHost, path);
        } else if (DELETE == method) {
            return webClient.delete(defaultPort, defaultHost, path);
        } else if (PUT == method) {
            return webClient.put(defaultPort, defaultHost, path);
        }
        Assertions.fail("Unsupported method");
        return null;
    }

    protected <T, R> R sendRequestWithBody(HttpRequest<Buffer> request, T entity, Class<R> responseClazz)
            throws InterruptedException {
        HttpResponse<Buffer> response = sendRequest(request, JsonMapper.jsonSerialize(entity));
        Assertions.assertEquals(HTTP_OK, response.statusCode());
        return JsonMapper.jsonDeserialize(response.bodyAsString(), responseClazz);
    }

    protected <T, R> R sendRequestWithBody(
            HttpRequest<Buffer> request, T entity, int statusCode, String statusMessage, Class<R> responseClazz
    )
            throws InterruptedException {
        HttpResponse<Buffer> response = sendRequest(request, JsonMapper.jsonSerialize(entity));
        Assertions.assertEquals(statusCode, response.statusCode());
        if (null != statusMessage) {
            Assertions.assertEquals(statusMessage, response.statusMessage());
        }
        if (null != responseClazz) {
            return JsonMapper.jsonDeserialize(response.bodyAsString(), responseClazz);
        }
        return null;
    }

    protected <R> R sendRequestWithoutBody(HttpRequest<Buffer> request, Class<R> responseClazz)
            throws InterruptedException {
        HttpResponse<Buffer> response = sendRequest(request, null);
        Assertions.assertEquals(HTTP_OK, response.statusCode());
        if (null != responseClazz) {
            return JsonMapper.jsonDeserialize(response.bodyAsString(), responseClazz);
        }
        return null;
    }

    protected <R> R sendRequestWithoutBody(
            HttpRequest<Buffer> request, int statusCode, String statusMessage, Class<R> responseClazz
    )
            throws InterruptedException {
        HttpResponse<Buffer> response = sendRequest(request, null);
        Assertions.assertEquals(statusCode, response.statusCode());
        if (null != statusMessage) {
            Assertions.assertEquals(statusMessage, response.statusMessage());
        }
        if (null != responseClazz) {
            return (R) JsonMapper.jsonDeserialize(response.bodyAsString(), responseClazz);
        }
        return null;
    }

    protected HttpResponse<Buffer> sendRequest(HttpRequest<Buffer> request, String json) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Future<HttpResponse<Buffer>> responseFuture;
        if (null != json) {
            Buffer reqBuffer = Buffer.buffer(json);
            responseFuture = request.sendBuffer(reqBuffer);
        } else {
            responseFuture = request.send();
        }

        class PostResponseCapture<T> {
            Throwable throwable;
            T response;
        }
        PostResponseCapture<HttpResponse<Buffer>> responseCapture = new PostResponseCapture<>();
        responseFuture.onComplete(r -> {
            responseCapture.response = r.result();
            responseCapture.throwable = r.cause();
            latch.countDown();
        });
        awaitLatch(latch);
        // post shouldn't fail.
        Assertions.assertNull(responseCapture.throwable);
        return responseCapture.response;
    }

    protected void awaitLatch(CountDownLatch latch) throws InterruptedException {
        awaitLatch(latch, 10, TimeUnit.SECONDS);
    }

    protected void awaitLatch(CountDownLatch latch, long timeout, TimeUnit unit) throws InterruptedException {
        assertTrue(latch.await(timeout, unit));
    }

    protected <T> Handler<AsyncResult<T>> onSuccess(Consumer<T> consumer) {
        return result -> {
            if (result.failed()) {
                result.cause().printStackTrace();
                fail(result.cause().getMessage());
            } else {
                consumer.accept(result.result());
            }
        };
    }

    protected void fail(String message) {
        Assertions.fail(message);
    }
}
