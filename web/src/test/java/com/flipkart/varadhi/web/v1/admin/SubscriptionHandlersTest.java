package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.web.ErrorResponse;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.entities.web.SubscriptionResource;
import com.flipkart.varadhi.web.WebTestBase;
import com.flipkart.varadhi.web.config.RestOptions;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.handler.HttpException;
import lombok.experimental.ExtensionMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.entities.Samples.PROJECT_1;
import static com.flipkart.varadhi.entities.Samples.U_TOPIC_RESOURCE_1;
import static com.flipkart.varadhi.entities.SubscriptionTestUtils.createSubscriptionResource;
import static com.flipkart.varadhi.entities.SubscriptionTestUtils.createUngroupedSubscription;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
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

@ExtensionMethod ({Extensions.RequestBodyExtension.class, Extensions.RoutingContextExtension.class})
class SubscriptionHandlersTest extends SubscriptionTestBase {

    private static final String BASE_PATH = "/projects/:project/subscriptions";
    private static final String SUBSCRIPTION_PATH = BASE_PATH + "/:subscription";

    private SubscriptionHandlers subscriptionHandlers;

    @Captor
    private ArgumentCaptor<String> stringCaptor;

    @Captor
    private ArgumentCaptor<Integer> integerCaptor;

    @BeforeEach
    public void PreTest() throws InterruptedException {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        subscriptionHandlers = new SubscriptionHandlers(
            subscriptionService,
            topicService,
            subscriptionFactory,
            new RestOptions(),
            projectCache
        );
        configureRoutes();
        Resource.EntityResource<Project> project = Resource.of(PROJECT_1, ResourceType.PROJECT);
        doReturn(project).when(projectCache).getOrThrow(PROJECT_1.getName());
    }

    private void configureRoutes() {
        createRoute(HttpMethod.POST, BASE_PATH, subscriptionHandlers::create, true);
        createRoute(HttpMethod.GET, SUBSCRIPTION_PATH, subscriptionHandlers::get, false);
        createRoute(HttpMethod.GET, BASE_PATH, subscriptionHandlers::list, false);
        createRoute(HttpMethod.DELETE, SUBSCRIPTION_PATH, subscriptionHandlers::delete, false);
        createRoute(HttpMethod.PUT, SUBSCRIPTION_PATH, subscriptionHandlers::update, true);
        createRoute(HttpMethod.PATCH, SUBSCRIPTION_PATH + "/restore", subscriptionHandlers::restore, false);
        createRoute(HttpMethod.POST, SUBSCRIPTION_PATH + "/start", subscriptionHandlers::start, false);
        createRoute(HttpMethod.POST, SUBSCRIPTION_PATH + "/stop", subscriptionHandlers::stop, false);
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
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, buildSubscriptionsUrl(PROJECT_1));
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT_1, U_TOPIC_RESOURCE_1);
        VaradhiTopic vTopic = U_TOPIC_RESOURCE_1.toVaradhiTopic();
        VaradhiSubscription subscription = createUngroupedSubscription("sub12", PROJECT_1, vTopic);

        doReturn(vTopic).when(topicService).get(U_TOPIC_RESOURCE_1.getProject() + "." + U_TOPIC_RESOURCE_1.getName());
        when(subscriptionService.createSubscription(any(), any(), any())).thenReturn(subscription);

        SubscriptionResource createdResource = sendRequestWithEntity(
            request,
            resource,
            WebTestBase.c(SubscriptionResource.class)
        );

        assertEquals(subscription.getName(), createdResource.getSubscriptionInternalName());
    }

    @Test
    void createSubscription_NonSuperUserWithIgnoreConstraints_ThrowsUnauthorizedException()
        throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(
            HttpMethod.POST,
            buildSubscriptionsUrl(PROJECT_1) + "?ignoreConstraints=true"
        );
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT_1, U_TOPIC_RESOURCE_1);
        String errorMessage = "ignoreConstraints is restricted to super admins only.";

        doThrow(new HttpException(HTTP_UNAUTHORIZED, errorMessage)).when(subscriptionService)
                                                                   .createSubscription(any(), any(), any());

        ErrorResponse response = sendRequestWithEntity(
            request,
            resource,
            401,
            errorMessage,
            WebTestBase.c(ErrorResponse.class)
        );

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void createSubscription_NonExistentProject_ThrowsNotFoundException() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, buildSubscriptionsUrl(PROJECT_1));
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT_1, U_TOPIC_RESOURCE_1);
        String errorMessage = "PROJECT(project1) not found";

        doThrow(new ResourceNotFoundException(errorMessage)).when(projectCache).getOrThrow(PROJECT_1.getName());

        ErrorResponse response = sendRequestWithEntity(
            request,
            resource,
            404,
            errorMessage,
            WebTestBase.c(ErrorResponse.class)
        );

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void createSubscription_NonExistentTopic_ThrowsNotFoundException() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, buildSubscriptionsUrl(PROJECT_1));
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT_1, U_TOPIC_RESOURCE_1);
        String errorMessage = "Topic not found.";

        doThrow(new ResourceNotFoundException(errorMessage)).when(topicService)
                                                            .get(
                                                                U_TOPIC_RESOURCE_1.getProject() + "."
                                                                 + U_TOPIC_RESOURCE_1.getName()
                                                            );

        ErrorResponse response = sendRequestWithEntity(
            request,
            resource,
            404,
            errorMessage,
            WebTestBase.c(ErrorResponse.class)
        );

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void createSubscription_MismatchedProjectName_ThrowsBadRequest() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, buildSubscriptionsUrl(PROJECT_1));
        SubscriptionResource resource = createSubscriptionResource(
            "sub1",
            Project.of("project2", "", "team1", "org1"),
            U_TOPIC_RESOURCE_1
        );
        String errorMessage = "Project name mismatch between URL and request body.";

        ErrorResponse response = sendRequestWithEntity(
            request,
            resource,
            400,
            errorMessage,
            WebTestBase.c(ErrorResponse.class)
        );

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void createSubscription_ExceedingRetryLimit_ThrowsBadRequest() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, buildSubscriptionsUrl(PROJECT_1));
        RetryPolicy retryPolicy = createCustomRetryPolicy(4);
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT_1, U_TOPIC_RESOURCE_1, retryPolicy);
        String errorMessage = "Only 3 retries are supported.";

        ErrorResponse response = sendRequestWithEntity(
            request,
            resource,
            400,
            errorMessage,
            WebTestBase.c(ErrorResponse.class)
        );

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void createSubscription_UnsupportedProperties_ThrowsBadRequest() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, buildSubscriptionsUrl(PROJECT_1));
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT_1, U_TOPIC_RESOURCE_1);
        resource.getProperties().put("unsupportedProperty", "value");
        String errorMessage = "Unsupported properties: unsupportedProperty";

        ErrorResponse response = sendRequestWithEntity(
            request,
            resource,
            400,
            errorMessage,
            WebTestBase.c(ErrorResponse.class)
        );

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void createSubscription_InvalidPropertyValues_ThrowsBadRequest() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, buildSubscriptionsUrl(PROJECT_1));
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT_1, U_TOPIC_RESOURCE_1);
        resource.getProperties().put("unsideline.api.message_count", "-10");
        String errorMessage = "Invalid value for property: unsideline.api.message_count";

        ErrorResponse response = sendRequestWithEntity(
            request,
            resource,
            400,
            errorMessage,
            WebTestBase.c(ErrorResponse.class)
        );

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void getSubscription_ValidSubscription_ReturnsSubscription() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, buildSubscriptionUrl("sub12", PROJECT_1));
        SubscriptionResource expectedResource = createSubscriptionResource("sub12", PROJECT_1, U_TOPIC_RESOURCE_1);
        VaradhiSubscription subscription = createUngroupedSubscription(
            "sub12",
            PROJECT_1,
            U_TOPIC_RESOURCE_1.toVaradhiTopic()
        );

        when(subscriptionService.getSubscription(anyString())).thenReturn(subscription);

        SubscriptionResource actualResource = sendRequestWithoutPayload(
            request,
            WebTestBase.c(SubscriptionResource.class)
        );

        assertEquals(actualResource.getName(), expectedResource.getName());
        verify(subscriptionService).getSubscription("project1.sub12");
    }

    @Test
    void listSubscriptions_ValidProject_ReturnsSubscriptionList() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, buildSubscriptionsUrl(PROJECT_1));

        when(subscriptionService.getSubscriptionList(PROJECT_1.getName(), false)).thenReturn(List.of("sub1", "sub2"))
                                                                                 .thenReturn(List.of());

        List<String> subscriptions = sendRequestWithoutPayload(request, WebTestBase.c(List.class));
        List<String> subscriptions2 = sendRequestWithoutPayload(request, WebTestBase.c(List.class));

        assertEquals(List.of("sub1", "sub2"), subscriptions);
        assertEquals(List.of(), subscriptions2);
        verify(subscriptionService, times(2)).getSubscriptionList(PROJECT_1.getName(), false);
    }

    @Test
    void listSubscriptions_IncludingInactive_ReturnsAllSubscriptions() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(
            HttpMethod.GET,
            buildSubscriptionsUrl(PROJECT_1) + "?includeInactive=true"
        );

        when(subscriptionService.getSubscriptionList(PROJECT_1.getName(), true)).thenReturn(
            List.of("sub1", "sub2", "sub3")
        );

        List<String> subscriptions = sendRequestWithoutPayload(request, WebTestBase.c(List.class));

        assertEquals(List.of("sub1", "sub2", "sub3"), subscriptions);
        verify(subscriptionService, times(1)).getSubscriptionList(PROJECT_1.getName(), true);
    }

    @Test
    void deleteSubscription_SoftDelete_Success() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(
            HttpMethod.DELETE,
            buildSubscriptionUrl("sub1", PROJECT_1) + "?deletionType=SOFT_DELETE"
        );

        doReturn(CompletableFuture.completedFuture(null)).when(subscriptionService)
                                                         .deleteSubscription(
                                                             anyString(),
                                                             eq(PROJECT_1),
                                                             any(),
                                                             eq(ResourceDeletionType.SOFT_DELETE),
                                                             any()
                                                         );

        sendRequestWithoutPayload(request, HTTP_NO_CONTENT);

        verify(subscriptionService, times(1)).deleteSubscription(
            eq("project1.sub1"),
            eq(PROJECT_1),
            any(),
            eq(ResourceDeletionType.SOFT_DELETE),
            any()
        );
    }

    @Test
    void deleteSubscription_HardDelete_Success() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(
            HttpMethod.DELETE,
            buildSubscriptionUrl("sub1", PROJECT_1) + "?deletionType=HARD_DELETE"
        );

        doReturn(CompletableFuture.completedFuture(null)).when(subscriptionService)
                                                         .deleteSubscription(
                                                             anyString(),
                                                             eq(PROJECT_1),
                                                             any(),
                                                             eq(ResourceDeletionType.HARD_DELETE),
                                                             any()
                                                         );

        sendRequestWithoutPayload(request, HTTP_NO_CONTENT);

        verify(subscriptionService, times(1)).deleteSubscription(
            eq("project1.sub1"),
            eq(PROJECT_1),
            any(),
            eq(ResourceDeletionType.HARD_DELETE),
            any()
        );
    }

    @Test
    void deleteSubscription_NoDeletionType_UsesSoftDelete() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.DELETE, buildSubscriptionUrl("sub1", PROJECT_1));

        doReturn(CompletableFuture.completedFuture(null)).when(subscriptionService)
                                                         .deleteSubscription(
                                                             anyString(),
                                                             eq(PROJECT_1),
                                                             any(),
                                                             eq(ResourceDeletionType.SOFT_DELETE),
                                                             any()
                                                         );

        sendRequestWithoutPayload(request, HTTP_NO_CONTENT);

        verify(subscriptionService, times(1)).deleteSubscription(
            eq("project1.sub1"),
            eq(PROJECT_1),
            any(),
            eq(ResourceDeletionType.SOFT_DELETE),
            any()
        );
    }

    @Test
    void deleteSubscription_InvalidDeletionType_UsesDefaultDeletionType() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(
            HttpMethod.DELETE,
            buildSubscriptionUrl("sub1", PROJECT_1) + "?deletionType=INVALID_TYPE"
        );

        doReturn(CompletableFuture.completedFuture(null)).when(subscriptionService)
                                                         .deleteSubscription(
                                                             anyString(),
                                                             eq(PROJECT_1),
                                                             any(),
                                                             eq(ResourceDeletionType.SOFT_DELETE),
                                                             any()
                                                         );

        sendRequestWithoutPayload(request, HTTP_NO_CONTENT);

        verify(subscriptionService, times(1)).deleteSubscription(
            eq("project1.sub1"),
            eq(PROJECT_1),
            any(),
            eq(ResourceDeletionType.SOFT_DELETE),
            any()
        );
    }

    @Test
    void updateSubscription_ValidRequest_ReturnsUpdatedSubscription() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.PUT, buildSubscriptionUrl("sub1", PROJECT_1));
        SubscriptionResource resource = createSubscriptionResource("sub1", PROJECT_1, U_TOPIC_RESOURCE_1);

        VaradhiTopic vTopic = U_TOPIC_RESOURCE_1.toVaradhiTopic();
        doReturn(vTopic).when(topicService).get(U_TOPIC_RESOURCE_1.getProject() + "." + U_TOPIC_RESOURCE_1.getName());

        VaradhiSubscription subscription = createUngroupedSubscription("sub1", PROJECT_1, vTopic);
        subscription.setVersion(2);
        doReturn(subscription).when(subscriptionFactory).get(any(), any(), any());

        when(
            subscriptionService.updateSubscription(
                stringCaptor.capture(),
                integerCaptor.capture(),
                anyString(),
                anyBoolean(),
                any(),
                any(),
                any(),
                any()
            )
        ).thenReturn(CompletableFuture.completedFuture(subscription));

        SubscriptionResource updated = sendRequestWithEntity(
            request,
            resource,
            WebTestBase.c(SubscriptionResource.class)
        );

        assertEquals(resource.getName(), updated.getName());
        assertEquals(resource.getSubscriptionInternalName(), stringCaptor.getValue());
        assertEquals(0, integerCaptor.getValue());
    }

    @Test
    void updateSubscription_ExceedingRetryLimit_ThrowsBadRequest() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.PUT, buildSubscriptionUrl("sub12", PROJECT_1));
        RetryPolicy retryPolicy = createCustomRetryPolicy(4); // Exceeding retry attempts
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT_1, U_TOPIC_RESOURCE_1, retryPolicy);

        String errorMessage = "Only 3 retries are supported.";

        ErrorResponse response = sendRequestWithEntity(
            request,
            resource,
            400,
            errorMessage,
            WebTestBase.c(ErrorResponse.class)
        );

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void updateSubscription_MismatchedProjectName_ThrowsBadRequest() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.PUT, buildSubscriptionUrl("sub12", PROJECT_1));
        SubscriptionResource resource = createSubscriptionResource(
            "sub12",
            Project.of("project2", "", "team1", "org1"),
            U_TOPIC_RESOURCE_1
        );

        String errorMessage = "Project name mismatch between URL and request body.";

        ErrorResponse response = sendRequestWithEntity(
            request,
            resource,
            400,
            errorMessage,
            WebTestBase.c(ErrorResponse.class)
        );

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void updateSubscription_NonSuperUserWithIgnoreConstraints_ThrowsUnauthorizedException()
        throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(
            HttpMethod.PUT,
            buildSubscriptionUrl("sub12", PROJECT_1) + "?ignoreConstraints=true"
        );
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT_1, U_TOPIC_RESOURCE_1);

        String errorMessage = "ignoreConstraints is restricted to super admins only.";
        doThrow(new HttpException(HTTP_UNAUTHORIZED, errorMessage)).when(subscriptionService)
                                                                   .updateSubscription(
                                                                       any(),
                                                                       anyInt(),
                                                                       anyString(),
                                                                       anyBoolean(),
                                                                       any(),
                                                                       any(),
                                                                       any(),
                                                                       any()
                                                                   );

        ErrorResponse response = sendRequestWithEntity(
            request,
            resource,
            401,
            errorMessage,
            WebTestBase.c(ErrorResponse.class)
        );

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void updateSubscription_UnsupportedProperties_ThrowsBadRequest() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.PUT, buildSubscriptionUrl("sub12", PROJECT_1));
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT_1, U_TOPIC_RESOURCE_1);
        resource.getProperties().put("unsupportedProperty", "value");

        String errorMessage = "Unsupported properties: unsupportedProperty";

        ErrorResponse response = sendRequestWithEntity(
            request,
            resource,
            400,
            errorMessage,
            WebTestBase.c(ErrorResponse.class)
        );

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void updateSubscription_InvalidPropertyValues_ThrowsBadRequest() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.PUT, buildSubscriptionUrl("sub12", PROJECT_1));
        SubscriptionResource resource = createSubscriptionResource("sub12", PROJECT_1, U_TOPIC_RESOURCE_1);
        resource.getProperties().put("unsideline.api.message_count", "-10");

        String errorMessage = "Invalid value for property: unsideline.api.message_count";

        ErrorResponse response = sendRequestWithEntity(
            request,
            resource,
            400,
            errorMessage,
            WebTestBase.c(ErrorResponse.class)
        );

        assertEquals(errorMessage, response.reason());
    }

    @Test
    void restoreSubscription_ValidRequest_ReturnsRestoredSubscription() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(
            HttpMethod.PATCH,
            buildSubscriptionUrl("sub1", PROJECT_1) + "/restore"
        );

        doReturn(CompletableFuture.completedFuture(null)).when(subscriptionService)
                                                         .restoreSubscription(any(), any(), any());

        sendRequestWithoutPayload(request, HTTP_NO_CONTENT);

        verify(subscriptionService, times(1)).restoreSubscription(any(), any(), any());
    }

    @Test
    void startSubscription_ValidRequest_TriggersStart() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(
            HttpMethod.POST,
            buildSubscriptionUrl("sub1", PROJECT_1) + "/start"
        );

        doReturn(CompletableFuture.completedFuture(null)).when(subscriptionService).start(any(), any());

        sendRequestWithoutPayload(request, HTTP_NO_CONTENT);

        verify(subscriptionService, times(1)).start(any(), any());
    }

    @Test
    void stopSubscription_ValidRequest_TriggersStop() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, buildSubscriptionUrl("sub1", PROJECT_1) + "/stop");

        doReturn(CompletableFuture.completedFuture(null)).when(subscriptionService).stop(any(), any());

        sendRequestWithoutPayload(request, HTTP_NO_CONTENT);

        verify(subscriptionService, times(1)).stop(any(), any());
    }
}
