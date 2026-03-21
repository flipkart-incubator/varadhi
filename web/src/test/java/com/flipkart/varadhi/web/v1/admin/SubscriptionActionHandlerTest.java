package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.entities.*;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.entities.Samples.PROJECT_1;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class SubscriptionActionHandlerTest extends SubscriptionTestBase {

    private static final String BASE_PATH = "/projects/:project/subscriptions";
    private static final String SUBSCRIPTION_PATH = BASE_PATH + "/:subscription";

    private SubscriptionActionHandler subscriptionActionHandler;

    @BeforeEach
    public void preTest() throws InterruptedException {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        subscriptionActionHandler = new SubscriptionActionHandler(varadhiSubscriptionService, projectCache);
        configureRoutes();
        Resource.EntityResource<Project> project = Resource.of(PROJECT_1, ResourceType.PROJECT);
        doReturn(project).when(projectCache).getOrThrow(PROJECT_1.getName());
    }

    private void configureRoutes() {
        createRoute(HttpMethod.POST, SUBSCRIPTION_PATH + "/start", subscriptionActionHandler::start, false);
        createRoute(HttpMethod.POST, SUBSCRIPTION_PATH + "/stop", subscriptionActionHandler::stop, false);
    }

    private void createRoute(HttpMethod method, String path, Handler<RoutingContext> handler, boolean requiresBody) {
        Route route = router.route(method, path);
        if (requiresBody) {
            route.handler(bodyHandler);
        }
        route.handler(wrapBlocking(handler));
        setupFailureHandler(route);
    }

    @AfterEach
    public void postTest() throws InterruptedException {
        super.tearDown();
    }

    @Test
    void startSubscription_ValidRequest_TriggersStart() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(
            HttpMethod.POST,
            buildSubscriptionUrl("sub1", PROJECT_1) + "/start"
        );

        doReturn(CompletableFuture.completedFuture(null)).when(varadhiSubscriptionService).start(any(), any());

        sendRequestWithoutPayload(request, HTTP_NO_CONTENT);

        verify(varadhiSubscriptionService, times(1)).start(any(), any());
    }

    @Test
    void stopSubscription_ValidRequest_TriggersStop() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, buildSubscriptionUrl("sub1", PROJECT_1) + "/stop");

        doReturn(CompletableFuture.completedFuture(null)).when(varadhiSubscriptionService).stop(any(), any());

        sendRequestWithoutPayload(request, HTTP_NO_CONTENT);

        verify(varadhiSubscriptionService, times(1)).stop(any(), any());
    }
}
