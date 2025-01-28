package com.flipkart.varadhi.web.admin;

import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.RetryPolicy;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.web.ErrorResponse;
import com.flipkart.varadhi.web.entities.SubscriptionResource;
import com.flipkart.varadhi.web.v1.admin.SubscriptionHandlers;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.handler.HttpException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SubscriptionHandlersTest extends SubscriptionTestBase {

    private SubscriptionHandlers subscriptionHandlers;

    @Captor
    private ArgumentCaptor<String> stringCaptor;

    @Captor
    private ArgumentCaptor<Integer> integerCaptor;

    @BeforeEach
    public void PreTest() throws InterruptedException {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        subscriptionHandlers =
                new SubscriptionHandlers(
                        subscriptionService, projectService, topicService, subscriptionFactory, new RestOptions());
        configureRoutes();
    }

    private void configureRoutes() {
        createRoute(
                HttpMethod.POST, "/projects/:project/subscriptions",
                subscriptionHandlers::create, true
        );
        createRoute(
                HttpMethod.GET, "/projects/:project/subscriptions/:subscription",
                subscriptionHandlers::get, false
        );
        createRoute(
                HttpMethod.GET, "/projects/:project/subscriptions",
                subscriptionHandlers::list, false
        );
        createRoute(
                HttpMethod.DELETE, "/projects/:project/subscriptions/:subscription",
                subscriptionHandlers::delete, false
        );
        createRoute(
                HttpMethod.PUT, "/projects/:project/subscriptions/:subscription",
                subscriptionHandlers::update, true
        );
        createRoute(
                HttpMethod.POST, "/projects/:project/subscriptions/:subscription/restore",
                subscriptionHandlers::restore, false
        );
        createRoute(
                HttpMethod.POST, "/projects/:project/subscriptions/:subscription/start",
                subscriptionHandlers::start, false
        );
        createRoute(
                HttpMethod.POST, "/projects/:project/subscriptions/:subscription/stop",
                subscriptionHandlers::stop, false
        );
    }

    private void createRoute(HttpMethod method, String path, Handler<RoutingContext> handler, boolean requiresBody) {
        Route route = router.route(method, path);
        if (requiresBody) {
            route.handler(bodyHandler).handler(ctx -> {
                subscriptionHandlers.setSubscription(ctx);
                ctx.next();
            });
        }
        route.handler(wrapBlocking(handler));
        setupFailureHandler(route);
    }

    @AfterEach
    public void PostTest() throws InterruptedException {
        super.tearDown();
    }

    @Test
    void createSubscription_ValidInput_CreatesSubscriptionSuccessfully() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, buildSubscriptionsUrl(PROJECT));
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT, TOPIC_RESOURCE);
        VaradhiTopic vTopic = TOPIC_RESOURCE.toVaradhiTopic();
        VaradhiSubscription subscription = createUngroupedSubscription("sub12", PROJECT, vTopic);

        doReturn(vTopic).when(topicService).get(TOPIC_RESOURCE.getProject() + "." + TOPIC_RESOURCE.getName());
        when(subscriptionService.createSubscription(any(), any(), any())).thenReturn(subscription);

        SubscriptionResource createdResource = sendRequestWithEntity(request, resource, SubscriptionResource.class);

        assertEquals(subscription.getName(), createdResource.getSubscriptionInternalName());
    }

    @Test
    void createSubscription_NonSuperUserWithIgnoreConstraints_ThrowsUnauthorizedException()
            throws InterruptedException {
        HttpRequest<Buffer> request =
                createRequest(HttpMethod.POST, buildSubscriptionsUrl(PROJECT) + "?ignoreConstraints=true");
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT, TOPIC_RESOURCE);
        String errorMessage = "ignoreConstraints is restricted to super admins only.";

        doThrow(new HttpException(HTTP_UNAUTHORIZED, errorMessage)).when(subscriptionService)
                .createSubscription(any(), any(), any());

        ErrorResponse response = sendRequestWithEntity(request, resource, 401, errorMessage, ErrorResponse.class);

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void createSubscription_NonExistentProject_ThrowsNotFoundException() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, buildSubscriptionsUrl(PROJECT));
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT, TOPIC_RESOURCE);
        String errorMessage = "Project not found.";

        doThrow(new ResourceNotFoundException(errorMessage)).when(projectService)
                .getCachedProject(PROJECT.getName());

        ErrorResponse response = sendRequestWithEntity(request, resource, 404, errorMessage, ErrorResponse.class);

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void createSubscription_NonExistentTopic_ThrowsNotFoundException() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, buildSubscriptionsUrl(PROJECT));
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT, TOPIC_RESOURCE);
        String errorMessage = "Topic not found.";

        doThrow(new ResourceNotFoundException(errorMessage)).when(topicService)
                .get(TOPIC_RESOURCE.getProject() + "." + TOPIC_RESOURCE.getName());

        ErrorResponse response = sendRequestWithEntity(request, resource, 404, errorMessage, ErrorResponse.class);

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void createSubscription_MismatchedProjectName_ThrowsBadRequest() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, buildSubscriptionsUrl(PROJECT));
        SubscriptionResource resource =
                createSubscriptionResource(
                        "sub1",
                        Project.of("project2", "", "team1", "org1"), TOPIC_RESOURCE
                );
        String errorMessage = "Project name mismatch between URL and request body.";

        ErrorResponse response = sendRequestWithEntity(request, resource, 400, errorMessage, ErrorResponse.class);

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void createSubscription_ExceedingRetryLimit_ThrowsBadRequest() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, buildSubscriptionsUrl(PROJECT));
        RetryPolicy retryPolicy = createCustomRetryPolicy(4);
        SubscriptionResource resource =
                createSubscriptionResource("sub12", PROJECT, TOPIC_RESOURCE, retryPolicy);
        String errorMessage = "Only 3 retries are supported.";

        ErrorResponse response = sendRequestWithEntity(request, resource, 400, errorMessage, ErrorResponse.class);

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void createSubscription_UnsupportedProperties_ThrowsBadRequest() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, buildSubscriptionsUrl(PROJECT));
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT, TOPIC_RESOURCE);
        resource.getProperties().put("unsupportedProperty", "value");
        String errorMessage = "Unsupported properties: unsupportedProperty";

        ErrorResponse response = sendRequestWithEntity(request, resource, 400, errorMessage, ErrorResponse.class);

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void createSubscription_InvalidPropertyValues_ThrowsBadRequest() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, buildSubscriptionsUrl(PROJECT));
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT, TOPIC_RESOURCE);
        resource.getProperties().put("unsideline.api.message_count", "-10");
        String errorMessage = "Invalid value for property: unsideline.api.message_count";

        ErrorResponse response = sendRequestWithEntity(request, resource, 400, errorMessage, ErrorResponse.class);

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void getSubscription_ValidSubscription_ReturnsSubscription() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, buildSubscriptionUrl("sub12", PROJECT));
        SubscriptionResource expectedResource = createSubscriptionResource("sub12", PROJECT, TOPIC_RESOURCE);
        VaradhiSubscription subscription =
                createUngroupedSubscription("sub12", PROJECT, TOPIC_RESOURCE.toVaradhiTopic());

        when(subscriptionService.getSubscription(anyString())).thenReturn(subscription);

        SubscriptionResource actualResource = sendRequestWithoutPayload(request, SubscriptionResource.class);

        assertEquals(actualResource.getName(), expectedResource.getName());
        verify(subscriptionService).getSubscription("project1.sub12");
    }

    @Test
    void listSubscriptions_ValidProject_ReturnsSubscriptionList() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, buildSubscriptionsUrl(PROJECT));

        when(subscriptionService.getSubscriptionList(PROJECT.getName()))
                .thenReturn(List.of("sub1", "sub2"))
                .thenReturn(List.of());

        List<String> subscriptions = sendRequestWithoutPayload(request, List.class);
        List<String> subscriptions2 = sendRequestWithoutPayload(request, List.class);

        assertEquals(List.of("sub1", "sub2"), subscriptions);
        assertEquals(List.of(), subscriptions2);
        verify(subscriptionService, times(2)).getSubscriptionList(PROJECT.getName());
    }

    @Test
    void deleteSubscription_SoftDelete_Success() throws InterruptedException {
        HttpRequest<Buffer> request =
                createRequest(
                        HttpMethod.DELETE,
                        buildSubscriptionUrl("sub1", PROJECT) + "?deletionType=SOFT_DELETE"
                );

        doReturn(PROJECT).when(projectService).getCachedProject(PROJECT.getName());
        doReturn(CompletableFuture.completedFuture(null)).when(subscriptionService)
                .deleteSubscription(anyString(), eq(PROJECT), any(), eq(ResourceDeletionType.SOFT_DELETE));

        sendRequestWithoutPayload(request, null);

        verify(subscriptionService, times(1)).deleteSubscription(
                eq("project1.sub1"), eq(PROJECT), any(), eq(ResourceDeletionType.SOFT_DELETE));
    }

    @Test
    void deleteSubscription_HardDelete_Success() throws InterruptedException {
        HttpRequest<Buffer> request =
                createRequest(
                        HttpMethod.DELETE,
                        buildSubscriptionUrl("sub1", PROJECT) + "?deletionType=HARD_DELETE"
                );

        doReturn(PROJECT).when(projectService).getCachedProject(PROJECT.getName());
        doReturn(CompletableFuture.completedFuture(null)).when(subscriptionService)
                .deleteSubscription(anyString(), eq(PROJECT), any(), eq(ResourceDeletionType.HARD_DELETE));

        sendRequestWithoutPayload(request, null);

        verify(subscriptionService, times(1)).deleteSubscription(
                eq("project1.sub1"), eq(PROJECT), any(), eq(ResourceDeletionType.HARD_DELETE));
    }

    @Test
    void deleteSubscription_NoDeletionType_UsesSoftDelete() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.DELETE, buildSubscriptionUrl("sub1", PROJECT));

        doReturn(PROJECT).when(projectService).getCachedProject(PROJECT.getName());
        doReturn(CompletableFuture.completedFuture(null)).when(subscriptionService)
                .deleteSubscription(
                        anyString(), eq(PROJECT), any(), eq(ResourceDeletionType.SOFT_DELETE));

        sendRequestWithoutPayload(request, null);

        verify(subscriptionService, times(1)).deleteSubscription(
                eq("project1.sub1"), eq(PROJECT), any(), eq(ResourceDeletionType.SOFT_DELETE));
    }

    @Test
    void deleteSubscription_InvalidDeletionType_UsesDefaultDeletionType() throws InterruptedException {
        HttpRequest<Buffer> request =
                createRequest(
                        HttpMethod.DELETE,
                        buildSubscriptionUrl("sub1", PROJECT) + "?deletionType=INVALID_TYPE"
                );

        doReturn(PROJECT).when(projectService).getCachedProject(PROJECT.getName());
        doReturn(CompletableFuture.completedFuture(null)).when(subscriptionService)
                .deleteSubscription(
                        anyString(), eq(PROJECT), any(), eq(ResourceDeletionType.DEFAULT));

        sendRequestWithoutPayload(request, null);

        verify(subscriptionService, times(1)).deleteSubscription(
                eq("project1.sub1"), eq(PROJECT), any(), eq(ResourceDeletionType.DEFAULT));
    }

    @Test
    void updateSubscription_ValidRequest_ReturnsUpdatedSubscription() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.PUT, buildSubscriptionUrl("sub1", PROJECT));
        SubscriptionResource resource = createSubscriptionResource("sub1", PROJECT, TOPIC_RESOURCE);

        VaradhiTopic vTopic = TOPIC_RESOURCE.toVaradhiTopic();
        doReturn(vTopic).when(topicService).get(TOPIC_RESOURCE.getProject() + "." + TOPIC_RESOURCE.getName());

        VaradhiSubscription subscription = createUngroupedSubscription("sub1", PROJECT, vTopic);
        subscription.setVersion(2);
        doReturn(subscription).when(subscriptionFactory).get(any(), any(), any());

        when(subscriptionService.updateSubscription(
                stringCaptor.capture(), integerCaptor.capture(), anyString(), anyBoolean(), any(), any(), any(),
                any()
        )).thenReturn(
                CompletableFuture.completedFuture(subscription));

        SubscriptionResource updated = sendRequestWithEntity(request, resource, SubscriptionResource.class);

        assertEquals(resource.getName(), updated.getName());
        assertEquals(resource.getSubscriptionInternalName(), stringCaptor.getValue());
        assertEquals(0, integerCaptor.getValue());
    }

    @Test
    void updateSubscription_ExceedingRetryLimit_ThrowsBadRequest() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.PUT, buildSubscriptionUrl("sub12", PROJECT));
        RetryPolicy retryPolicy = createCustomRetryPolicy(4); // Exceeding retry attempts
        SubscriptionResource resource =
                createSubscriptionResource("sub12", PROJECT, TOPIC_RESOURCE, retryPolicy);

        String errorMessage = "Only 3 retries are supported.";

        ErrorResponse response = sendRequestWithEntity(request, resource, 400, errorMessage, ErrorResponse.class);

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void updateSubscription_MismatchedProjectName_ThrowsBadRequest() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.PUT, buildSubscriptionUrl("sub12", PROJECT));
        SubscriptionResource resource =
                createSubscriptionResource(
                        "sub12",
                        Project.of("project2", "", "team1", "org1"), TOPIC_RESOURCE
                );

        String errorMessage = "Project name mismatch between URL and request body.";

        ErrorResponse response = sendRequestWithEntity(request, resource, 400, errorMessage, ErrorResponse.class);

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void updateSubscription_NonSuperUserWithIgnoreConstraints_ThrowsUnauthorizedException()
            throws InterruptedException {
        HttpRequest<Buffer> request =
                createRequest(
                        HttpMethod.PUT,
                        buildSubscriptionUrl("sub12", PROJECT) + "?ignoreConstraints=true"
                );
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT, TOPIC_RESOURCE);

        String errorMessage = "ignoreConstraints is restricted to super admins only.";
        doThrow(new HttpException(HTTP_UNAUTHORIZED, errorMessage)).when(subscriptionService)
                .updateSubscription(any(), anyInt(), anyString(), anyBoolean(), any(), any(), any(), any());

        ErrorResponse response = sendRequestWithEntity(request, resource, 401, errorMessage, ErrorResponse.class);

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void updateSubscription_UnsupportedProperties_ThrowsBadRequest() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.PUT, buildSubscriptionUrl("sub12", PROJECT));
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT, TOPIC_RESOURCE);
        resource.getProperties().put("unsupportedProperty", "value");

        String errorMessage = "Unsupported properties: unsupportedProperty";

        ErrorResponse response = sendRequestWithEntity(request, resource, 400, errorMessage, ErrorResponse.class);

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void updateSubscription_InvalidPropertyValues_ThrowsBadRequest() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.PUT, buildSubscriptionUrl("sub12", PROJECT));
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT, TOPIC_RESOURCE);
        resource.getProperties().put("unsideline.api.message_count", "-10");

        String errorMessage = "Invalid value for property: unsideline.api.message_count";

        ErrorResponse response = sendRequestWithEntity(request, resource, 400, errorMessage, ErrorResponse.class);

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void restoreSubscription_ValidRequest_ReturnsRestoredSubscription() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(
                HttpMethod.POST,
                buildSubscriptionUrl("sub1", PROJECT) + "/restore"
        );
        SubscriptionResource resource = createSubscriptionResource("sub1", PROJECT, TOPIC_RESOURCE);
        VaradhiSubscription subscription =
                createUngroupedSubscription("sub1", PROJECT, TOPIC_RESOURCE.toVaradhiTopic());

        doReturn(CompletableFuture.completedFuture(subscription)).when(subscriptionService)
                .restoreSubscription(any(), any());

        SubscriptionResource restored = sendRequestWithoutPayload(request, SubscriptionResource.class);

        assertEquals(resource.getName(), restored.getName());
        verify(subscriptionService, times(1)).restoreSubscription(any(), any());
    }

    @Test
    void startSubscription_ValidRequest_TriggersStart() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(
                HttpMethod.POST,
                buildSubscriptionUrl("sub1", PROJECT) + "/start"
        );

        doReturn(CompletableFuture.completedFuture(null)).when(subscriptionService).start(any(), any());

        sendRequestWithoutPayload(request, null);

        verify(subscriptionService, times(1)).start(any(), any());
    }

    @Test
    void stopSubscription_ValidRequest_TriggersStop() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(
                HttpMethod.POST,
                buildSubscriptionUrl("sub1", PROJECT) + "/stop"
        );

        doReturn(CompletableFuture.completedFuture(null)).when(subscriptionService).stop(any(), any());

        sendRequestWithoutPayload(request, null);

        verify(subscriptionService, times(1)).stop(any(), any());
    }
}
