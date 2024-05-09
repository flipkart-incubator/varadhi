package com.flipkart.varadhi.web.admin;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.services.SubscriptionService;
import com.flipkart.varadhi.utils.SubscriptionHelper;
import com.flipkart.varadhi.web.ErrorResponse;
import com.flipkart.varadhi.web.WebTestBase;
import com.flipkart.varadhi.web.v1.admin.SubscriptionHandlers;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.client.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class SubscriptionHandlersTest extends WebTestBase {
    private final Project project = new Project("project1", 0, "", "team1", "org1");
    private final TopicResource topicResource = new TopicResource("topic1", 0, "project2", false, null);
    private static final Endpoint endpoint;
    private static final RetryPolicy retryPolicy = new RetryPolicy(
            new CodeRange[]{new CodeRange(500, 502)},
            RetryPolicy.BackoffType.LINEAR,
            1, 1, 1, 1
    );
    private static final ConsumptionPolicy consumptionPolicy = new ConsumptionPolicy(1, 1, false, 1, null);
    private static final MemberResources requests = new MemberResources(0, 10);
    private static final SubscriptionShards shards = new SubscriptionUnitShard(0, requests,null, null);

    static {
        try {
            endpoint = new Endpoint.HttpEndpoint(new URL("http", "localhost", "hello"), "GET", "", 500, 500, false);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
    SubscriptionHandlers subscriptionHandlers;
    SubscriptionService subscriptionService;
    ProjectService projectService;

    public static VaradhiSubscription getVaradhiSubscription(
            String subscriptionName, Project project, VaradhiTopic topic
    ) {
        return new VaradhiSubscription(
                SubscriptionHelper.buildSubscriptionName(project.getName(), subscriptionName),
                VersionedEntity.INITIAL_VERSION,
                project.getName(),
                topic.getName(),
                UUID.randomUUID().toString(),
                false,
                endpoint,
                retryPolicy,
                consumptionPolicy,
                shards
        );
    }

    public static VaradhiSubscription getVaradhiSubscription(
            String subscriptionName, boolean grouped, Project project, VaradhiTopic topic
    ) {
        return new VaradhiSubscription(
                SubscriptionHelper.buildSubscriptionName(project.getName(), subscriptionName),
                VersionedEntity.INITIAL_VERSION,
                project.getName(),
                topic.getName(),
                UUID.randomUUID().toString(),
                grouped,
                endpoint,
                retryPolicy,
                consumptionPolicy,
                shards
        );
    }

    @BeforeEach
    public void PreTest() throws InterruptedException {
        super.setUp();
        subscriptionService = mock(SubscriptionService.class);
        projectService = mock(ProjectService.class);
        subscriptionHandlers = new SubscriptionHandlers(subscriptionService, projectService);

        Route routeCreate = router.post("/projects/:project/subscriptions").handler(bodyHandler).handler(ctx -> {
                    subscriptionHandlers.setSubscription(ctx);
                    ctx.next();
                })
                .handler(wrapBlocking(subscriptionHandlers::create));
        setupFailureHandler(routeCreate);

        Route routeGet = router.get("/projects/:project/subscriptions/:subscription")
                .handler(wrapBlocking(subscriptionHandlers::get));
        setupFailureHandler(routeGet);

        Route routeListAll =
                router.get("/projects/:project/subscriptions").handler(wrapBlocking(subscriptionHandlers::list));
        setupFailureHandler(routeListAll);

        Route routeDelete = router.delete("/projects/:project/subscriptions/:subscription")
                .handler(wrapBlocking(subscriptionHandlers::delete));
        setupFailureHandler(routeDelete);

        Route routeUpdate = router.put("/projects/:project/subscriptions/:subscription").handler(bodyHandler)
                .handler(ctx -> {
                    subscriptionHandlers.setSubscription(ctx);
                    ctx.next();
                })
                .handler(wrapBlocking(subscriptionHandlers::update));
        setupFailureHandler(routeUpdate);
    }

    @AfterEach
    public void PostTest() throws InterruptedException {
        super.tearDown();
    }

    @Test
    void testSubscriptionCreate() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, getSubscriptionsUrl(project));
        SubscriptionResource resource = getSubscriptionResource("sub12", project, topicResource);

        VaradhiSubscription subscription = getVaradhiSubscription("sub12", project, VaradhiTopic.of(topicResource));
        when(subscriptionService.createSubscription(any())).thenReturn(subscription);
        SubscriptionResource created = sendRequestWithBody(request, resource, SubscriptionResource.class);
        assertEquals(
                subscription.getName(),
                SubscriptionHelper.buildSubscriptionName(created.getProject(), created.getName())
        );
    }

    @Test
    void testSubscriptionCreateInconsistentProjectNameFailure() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, getSubscriptionsUrl(project));
        SubscriptionResource resource =
                getSubscriptionResource("sub1", new Project("project2", 0, "", "team1", "org1"), topicResource);

        String errMsg = "Specified Project name is different from Project name in url";
        ErrorResponse resp = sendRequestWithBody(request, resource, 400, errMsg, ErrorResponse.class);
        assertEquals(errMsg, resp.reason());
    }

    @Test
    void testSubscriptionGet() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, getSubscriptionUrl("sub12", project));
        SubscriptionResource resource = getSubscriptionResource("sub12", project, topicResource);

        VaradhiSubscription subscription = getVaradhiSubscription("sub12", project, VaradhiTopic.of(topicResource));
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        when(subscriptionService.getSubscription(captor.capture())).thenReturn(subscription);

        SubscriptionResource got = sendRequestWithoutBody(request, SubscriptionResource.class);
        assertEquals(got.getName(), resource.getName());
        assertEquals(
                captor.getValue(), SubscriptionHelper.buildSubscriptionName(project.getName(), resource.getName()));
    }

    @Test
    void testListSubscription() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, getSubscriptionsUrl(project));

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        when(subscriptionService.getSubscriptionList(captor.capture()))
                .thenReturn(List.of("sub1", "sub2"))
                .thenReturn(List.of());

        List<String> got = sendRequestWithoutBody(request, List.class);
        assertEquals(List.of("sub1", "sub2"), got);
        assertEquals(project.getName(), captor.getValue());

        List<String> got2 = sendRequestWithoutBody(request, List.class);
        assertEquals(List.of(), got2);
    }

    @Test
    void testSubscriptionDelete() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.DELETE, getSubscriptionUrl("sub1", project));
        SubscriptionResource resource = getSubscriptionResource("sub1", project, topicResource);

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        doNothing().when(subscriptionService).deleteSubscription(captor.capture());

        sendRequestWithoutBody(request, null);
        assertEquals(
                captor.getValue(), SubscriptionHelper.buildSubscriptionName(project.getName(), resource.getName()));
        verify(subscriptionService, times(1)).deleteSubscription(any());
    }

    @Test
    void testSubscriptionUpdate() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.PUT, getSubscriptionUrl("sub1", project));
        SubscriptionResource resource = getSubscriptionResource("sub1", project, topicResource);

        VaradhiSubscription subscription = getVaradhiSubscription("sub1", project, VaradhiTopic.of(topicResource));
        ArgumentCaptor<VaradhiSubscription> captor = ArgumentCaptor.forClass(VaradhiSubscription.class);
        when(subscriptionService.updateSubscription(captor.capture())).thenReturn(subscription);

        SubscriptionResource updated = sendRequestWithBody(request, resource, SubscriptionResource.class);
        assertEquals(updated.getName(), resource.getName());
        assertEquals(
                captor.getValue().getName(),
                SubscriptionHelper.buildSubscriptionName(project.getName(), resource.getName())
        );
    }

    private String getSubscriptionsUrl(Project project) {
        return String.join("/", "/projects", project.getName(), "subscriptions");
    }

    private String getSubscriptionUrl(String subscriptionName, Project project) {
        return String.join("/", getSubscriptionsUrl(project), subscriptionName);
    }

    private SubscriptionResource getSubscriptionResource(
            String subscriptionName, Project project, TopicResource topic
    ) {
        return new SubscriptionResource(
                subscriptionName,
                1,
                project.getName(),
                topic.getName(),
                topic.getProject(),
                "desc",
                false,
                endpoint,
                retryPolicy,
                consumptionPolicy
        );
    }

}
