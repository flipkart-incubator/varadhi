package com.flipkart.varadhi.web.admin;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.MetaStoreEntityType;
import com.flipkart.varadhi.entities.cluster.SubscriptionOperation;
import com.flipkart.varadhi.pulsar.entities.PulsarOffset;
import com.flipkart.varadhi.services.DlqService;
import com.flipkart.varadhi.common.utils.JsonMapper;
import com.flipkart.varadhi.web.ErrorResponse;
import com.flipkart.varadhi.web.entities.DlqMessagesResponse;
import com.flipkart.varadhi.web.entities.DlqPageMarker;
import com.flipkart.varadhi.web.entities.SubscriptionResource;
import com.flipkart.varadhi.web.v1.admin.DlqHandlers;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.client.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.flipkart.varadhi.entities.Constants.SubscriptionProperties.UNSIDELINE_API_GROUP_COUNT;
import static com.flipkart.varadhi.entities.Constants.SubscriptionProperties.UNSIDELINE_API_MESSAGE_COUNT;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;

public class DlqHandlersTest extends SubscriptionTestBase {
    private static final char RECORD_SEPARATOR = '\u001E';
    private DlqService dlqService;
    private DlqHandlers dlqHandlers;
    ObjectMapper objectMapper;

    @BeforeEach
    public void PreTest() throws InterruptedException {
        super.setUp();
        dlqService = mock(DlqService.class);
        dlqHandlers = new DlqHandlers(dlqService, subscriptionService, projectCache);
        Route routeUnsideline = router.post("/projects/:project/subscriptions/:subscription/dlq/messages/unsideline")
                                      .handler(bodyHandler)
                                      .handler(ctx -> {
                                          dlqHandlers.setUnsidelineRequest(ctx);
                                          ctx.next();
                                      })
                                      .handler(dlqHandlers::enqueueUnsideline);
        setupFailureHandler(routeUnsideline);

        Route routeGetMessages = router.get("/projects/:project/subscriptions/:subscription/dlq/messages")
                                       .handler(dlqHandlers::listMessages);
        setupFailureHandler(routeGetMessages);
        objectMapper = JsonMapper.getMapper();
        objectMapper.registerSubtypes(new NamedType(PulsarOffset.class, "PulsarOffset"));
    }

    @AfterEach
    public void PostTest() throws InterruptedException {
        super.tearDown();
    }

    private VaradhiSubscription prepUnsidelineRequest(
        UnsidelineRequest unsidelineRequest,
        ArgumentCaptor<UnsidelineRequest> captor
    ) {
        SubscriptionResource subResource = createSubscriptionResource("sub12", PROJECT, TOPIC_RESOURCE);
        VaradhiTopic vTopic = TOPIC_RESOURCE.toVaradhiTopic();
        VaradhiSubscription subscription = createUngroupedSubscription("sub12", PROJECT, vTopic);
        Resource.EntityResource<Project> project = Resource.of(PROJECT, MetaStoreEntityType.PROJECT, ResourceType.PROJECT);
        doReturn(project).when(projectCache).getOrThrow(PROJECT.getName());
        doReturn(subscription).when(subscriptionService).getSubscription(subResource.getSubscriptionInternalName());
        SubscriptionOperation op = SubscriptionOperation.unsidelineOp(
            subscription.getName(),
            unsidelineRequest,
            "foobar"
        );
        CompletableFuture<SubscriptionOperation> future = CompletableFuture.completedFuture(op);
        when(dlqService.unsideline(eq(subscription), captor.capture(), any())).thenReturn(future);
        return subscription;
    }

    @Test
    void dlqUnsidelineOfTimeStamp() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, getUnsidelineUrl("sub12", PROJECT));
        ArgumentCaptor<UnsidelineRequest> captor = ArgumentCaptor.forClass(UnsidelineRequest.class);
        UnsidelineRequest unsidelineRequest = UnsidelineRequest.ofFailedAt(System.currentTimeMillis());
        prepUnsidelineRequest(unsidelineRequest, captor);
        SubscriptionOperation operation = sendRequestWithEntity(
            request,
            unsidelineRequest,
            SubscriptionOperation.class
        );
        UnsidelineRequest actual = captor.getValue();
        assertEquals(unsidelineRequest.getLatestFailedAt(), actual.getLatestFailedAt());
        assertEquals(unsidelineRequest.getMessageIds().size(), 0);
        assertEquals(unsidelineRequest.getGroupIds().size(), 0);
        assertNotNull(operation);
    }

    @Test
    void dlqUnsidelineOfGroupIds() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, getUnsidelineUrl("sub12", PROJECT));
        ArgumentCaptor<UnsidelineRequest> captor = ArgumentCaptor.forClass(UnsidelineRequest.class);
        UnsidelineRequest unsidelineRequest = UnsidelineRequest.ofGroupIds(List.of("grp1", "grp2"));
        VaradhiSubscription subscription = prepUnsidelineRequest(unsidelineRequest, captor);
        subscription.getProperties().put(UNSIDELINE_API_GROUP_COUNT, "5");
        sendRequestWithEntity(
            request,
            unsidelineRequest,
            400,
            "Selective unsideline is not yet supported.",
            ErrorResponse.class
        );
        verify(dlqService, never()).unsideline(any(), any(), any());
    }

    @Test
    void dlqUnsidelineOfHigherGroupCount() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, getUnsidelineUrl("sub12", PROJECT));
        ArgumentCaptor<UnsidelineRequest> captor = ArgumentCaptor.forClass(UnsidelineRequest.class);
        UnsidelineRequest unsidelineRequest = UnsidelineRequest.ofGroupIds(List.of("grp1", "grp2"));
        VaradhiSubscription subscription = prepUnsidelineRequest(unsidelineRequest, captor);
        subscription.getProperties().put(UNSIDELINE_API_GROUP_COUNT, "1");
        sendRequestWithEntity(
            request,
            unsidelineRequest,
            400,
            "Number of groupIds in one API call cannot be more than 1.",
            ErrorResponse.class
        );
        verify(dlqService, never()).unsideline(any(), any(), any());
    }

    @Test
    void dlqUnsidelineOfMessageIds() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, getUnsidelineUrl("sub12", PROJECT));
        ArgumentCaptor<UnsidelineRequest> captor = ArgumentCaptor.forClass(UnsidelineRequest.class);
        UnsidelineRequest unsidelineRequest = UnsidelineRequest.ofMessageIds(List.of("mid1", "mid2"));
        VaradhiSubscription subscription = prepUnsidelineRequest(unsidelineRequest, captor);
        subscription.getProperties().put(UNSIDELINE_API_MESSAGE_COUNT, "5");
        sendRequestWithEntity(
            request,
            unsidelineRequest,
            400,
            "Selective unsideline is not yet supported.",
            ErrorResponse.class
        );
        verify(dlqService, never()).unsideline(any(), any(), any());
    }

    @Test
    void dlqUnsidelineOfHigherMessageCount() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, getUnsidelineUrl("sub12", PROJECT));
        ArgumentCaptor<UnsidelineRequest> captor = ArgumentCaptor.forClass(UnsidelineRequest.class);
        UnsidelineRequest unsidelineRequest = UnsidelineRequest.ofMessageIds(List.of("mid1", "mid2"));
        VaradhiSubscription subscription = prepUnsidelineRequest(unsidelineRequest, captor);
        subscription.getProperties().put(UNSIDELINE_API_MESSAGE_COUNT, "1");
        sendRequestWithEntity(
            request,
            unsidelineRequest,
            400,
            "Number of messageIds in one API call cannot be more than 1.",
            ErrorResponse.class
        );
        verify(dlqService, never()).unsideline(any(), any(), any());
    }

    @Test
    void dlqUnsidelineNoCriteria() throws InterruptedException {
        HttpRequest<Buffer> request = createRequest(HttpMethod.POST, getUnsidelineUrl("sub12", PROJECT));
        ArgumentCaptor<UnsidelineRequest> captor = ArgumentCaptor.forClass(UnsidelineRequest.class);
        UnsidelineRequest unsidelineRequest = UnsidelineRequest.ofFailedAt(UnsidelineRequest.UNSPECIFIED_TS);
        VaradhiSubscription subscription = prepUnsidelineRequest(unsidelineRequest, captor);
        subscription.getProperties().put(UNSIDELINE_API_MESSAGE_COUNT, "1");
        sendRequestWithEntity(
            request,
            unsidelineRequest,
            400,
            "At least one unsideline criteria needs to be specified.",
            ErrorResponse.class
        );
        verify(dlqService, never()).unsideline(any(), any(), any());
    }

    @Test
    void testDlqListMessages() throws Exception {
        HttpRequest<Buffer> request = createRequest(
            HttpMethod.GET,
            listMessagesUrl("sub12", PROJECT, System.currentTimeMillis(), "", -1)
        );
        ArgumentCaptor<DlqPageMarker> captor = ArgumentCaptor.forClass(DlqPageMarker.class);
        VaradhiSubscription subscription = setupSubscriptionForListMessages();

        List<DlqMessage> shard1Messages = List.of(createDlqMessage(1), createDlqMessage(2), createDlqMessage(1));
        List<DlqMessage> shard2Messages = List.of(createDlqMessage(3), createDlqMessage(3), createDlqMessage(4));
        DlqPageMarker pageMarker = new DlqPageMarker(new HashMap<>());
        pageMarker.addShardMarker(
            1,
            shard1Messages.get(1).getOffset().toString() + "," + shard1Messages.get(2).getOffset().toString()
        );
        pageMarker.addShardMarker(
            2,
            shard1Messages.get(1).getOffset().toString() + "," + shard1Messages.get(2).getOffset().toString()
        );

        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        doAnswer(invocationOnMock -> {
            Consumer<DlqMessagesResponse> responseWriter = invocationOnMock.getArgument(4);
            responseWriter.accept(DlqMessagesResponse.of(shard1Messages));
            responseWriter.accept(DlqMessagesResponse.of(shard2Messages));
            responseWriter.accept(DlqMessagesResponse.of(pageMarker, new ArrayList<>()));
            return future;
        }).when(dlqService).getMessages(eq(subscription), anyLong(), captor.capture(), anyInt(), any());
        byte[] response = sendRequestWithoutPayload(request);
        List<DlqMessagesResponse> dlqResponses = readMessageResponse(response);
        assertEquals(3, dlqResponses.size());
        assertEquals(pageMarker.toString(), dlqResponses.get(2).getNextPage());
        assertFalse(captor.getValue().hasMarkers());
    }

    @Test
    void testDlqListMessagesFromMarker() throws Exception {
        List<DlqMessage> shard1Messages = List.of(createDlqMessage(1), createDlqMessage(2), createDlqMessage(1));
        List<DlqMessage> shard2Messages = List.of(createDlqMessage(3), createDlqMessage(3), createDlqMessage(4));
        DlqPageMarker pageMarker = new DlqPageMarker(new HashMap<>());
        pageMarker.addShardMarker(
            1,
            shard1Messages.get(1).getOffset().toString() + "," + shard1Messages.get(2).getOffset().toString()
        );
        pageMarker.addShardMarker(
            2,
            shard1Messages.get(1).getOffset().toString() + "," + shard1Messages.get(2).getOffset().toString()
        );

        HttpRequest<Buffer> request = createRequest(
            HttpMethod.GET,
            listMessagesUrl("sub12", PROJECT, 0, pageMarker.toString(), -1)
        );
        ArgumentCaptor<DlqPageMarker> captor = ArgumentCaptor.forClass(DlqPageMarker.class);
        VaradhiSubscription subscription = setupSubscriptionForListMessages();

        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        doAnswer(invocationOnMock -> {
            Consumer<DlqMessagesResponse> responseWriter = invocationOnMock.getArgument(4);
            responseWriter.accept(DlqMessagesResponse.of(shard1Messages));
            responseWriter.accept(DlqMessagesResponse.of(shard2Messages));
            responseWriter.accept(DlqMessagesResponse.of(DlqPageMarker.fromString(""), new ArrayList<>()));
            return future;
        }).when(dlqService).getMessages(eq(subscription), anyLong(), captor.capture(), anyInt(), any());
        byte[] response = sendRequestWithoutPayload(request);
        List<DlqMessagesResponse> dlqResponses = readMessageResponse(response);
        assertEquals(3, dlqResponses.size());
        assertEquals(pageMarker.toString(), captor.getValue().toString());
        assertTrue(dlqResponses.get(2).getNextPage().isBlank());
    }


    @Test
    void testDlqListMessagesInvalidLimit() throws Exception {
        HttpRequest<Buffer> request = createRequest(
            HttpMethod.GET,
            listMessagesUrl("sub12", PROJECT, System.currentTimeMillis(), "", 5000)
        );
        setupSubscriptionForListMessages();
        sendRequestWithoutPayload(request, 400, "Limit cannot be more than 100.");
    }

    @Test
    void testDlqListMessagesNoCriteria() throws Exception {
        HttpRequest<Buffer> request = createRequest(HttpMethod.GET, listMessagesUrl("sub12", PROJECT, 0, "", -1));
        setupSubscriptionForListMessages();
        sendRequestWithoutPayload(request, 400, "At least one get messages criteria needs to be specified.");
    }

    @Test
    void testDlqListMessagesMultipleCriteria() throws Exception {
        HttpRequest<Buffer> request = createRequest(
            HttpMethod.GET,
            listMessagesUrl("sub12", PROJECT, System.currentTimeMillis(), "1=mId:1:2:3", -1)
        );
        setupSubscriptionForListMessages();
        sendRequestWithoutPayload(request, 400, "Only one of the get messages criteria should be specified.");
    }

    @Test
    void testDlqListMessagesInvalidNextPageMarker() throws Exception {
        HttpRequest<Buffer> request = createRequest(
            HttpMethod.GET,
            listMessagesUrl("sub12", PROJECT, System.currentTimeMillis(), "mId:1:2:3", -1)
        );
        setupSubscriptionForListMessages();
        sendRequestWithoutPayload(request, 400, "Invalid page marker: mId:1:2:3");
    }

    private VaradhiSubscription setupSubscriptionForListMessages() {
        SubscriptionResource subResource = createSubscriptionResource("sub12", PROJECT, TOPIC_RESOURCE);
        VaradhiTopic vTopic = TOPIC_RESOURCE.toVaradhiTopic();
        VaradhiSubscription subscription = createUngroupedSubscription("sub12", PROJECT, vTopic);
        Resource.EntityResource<Project> project = Resource.of(PROJECT, MetaStoreEntityType.PROJECT, ResourceType.PROJECT);
        doReturn(project).when(projectCache).getOrThrow(PROJECT.getName());
        doReturn(subscription).when(subscriptionService).getSubscription(subResource.getSubscriptionInternalName());
        return subscription;
    }

    private List<DlqMessagesResponse> readMessageResponse(byte[] response) throws Exception {
        List<DlqMessagesResponse> dlqResponses = new ArrayList<>();
        int startIndex = 0;

        for (int i = 0; i < response.length; i++) {
            if (response[i] == RECORD_SEPARATOR) {
                if (startIndex < i) {
                    JsonNode jsonNode = objectMapper.readTree(response, startIndex, i - startIndex);
                    System.out.println(jsonNode);
                    DlqMessagesResponse dlqResponse = objectMapper.treeToValue(jsonNode, DlqMessagesResponse.class);
                    dlqResponses.add(dlqResponse);
                }
                startIndex = i + 1;
            }
        }

        if (startIndex < response.length) {
            JsonNode jsonNode = objectMapper.readTree(response, startIndex, response.length - startIndex);
            DlqMessagesResponse dlqResponse = objectMapper.treeToValue(jsonNode, DlqMessagesResponse.class);
            dlqResponses.add(dlqResponse);
        }
        return dlqResponses;
    }



    private String getUnsidelineUrl(String subscriptionName, Project project) {
        return String.join("/", buildSubscriptionUrl(subscriptionName, project), "dlq", "messages", "unsideline");
    }

    private String listMessagesUrl(
        String subscriptionName,
        Project project,
        long failedAt,
        String nextPage,
        int limit
    ) {
        StringBuilder sb = new StringBuilder(
            String.join("/", buildSubscriptionUrl(subscriptionName, project), "dlq", "messages")
        );
        ArrayList<String> queryParams = new ArrayList<>();
        if (0 != failedAt) {
            queryParams.add(String.format("earliestFailedAt=%d", failedAt));
        }
        if (-1 != limit) {
            queryParams.add(String.format("limit=%d", limit));
        }
        if (null != nextPage && !nextPage.isBlank()) {
            queryParams.add(String.format("nextPage=%s", nextPage));
        }
        if (!queryParams.isEmpty()) {
            sb.append("?");
            sb.append(String.join("&&", queryParams));
        }
        return sb.toString();
    }
}
