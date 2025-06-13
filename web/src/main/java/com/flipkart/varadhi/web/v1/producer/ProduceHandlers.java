package com.flipkart.varadhi.web.v1.producer;

import com.flipkart.varadhi.common.Constants;
import com.flipkart.varadhi.common.Constants.HttpCodes;
import com.flipkart.varadhi.common.Constants.PathParams;
import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.common.SimpleMessage;
import com.flipkart.varadhi.core.config.MessageConfiguration;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.auth.ResourceAction;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.produce.ProducerService;
import com.flipkart.varadhi.web.Extensions.RequestBodyExtension;
import com.flipkart.varadhi.web.Extensions.RoutingContextExtension;
import com.flipkart.varadhi.web.MessageRequestValidator;
import com.flipkart.varadhi.web.hierarchy.Hierarchies;
import com.flipkart.varadhi.web.hierarchy.ResourceHierarchy;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.vertx.core.MultiMap;
import io.vertx.ext.web.RoutingContext;
import lombok.RequiredArgsConstructor;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;

/**
 * Handles HTTP requests for message production to topics in the Varadhi messaging system.
 * This class implements the RouteProvider interface to define routes for message production endpoints.
 *
 * <p>The handler supports:
 * <ul>
 *   <li>Message production to topics with metrics tracking</li>
 *   <li>Header validation and compliance checking</li>
 *   <li>Authorization and access control</li>
 *   <li>Error handling and status code mapping</li>
 * </ul>
 */
@Slf4j
@ExtensionMethod ({RequestBodyExtension.class, RoutingContextExtension.class})
@RequiredArgsConstructor
public class ProduceHandlers implements RouteProvider {
    private static final String API_NAME = "TOPIC";
    private final ProducerService producerService;
    private final MessageConfiguration msgConfig;
    private final String produceRegion;
    private final ResourceReadCache<Resource.EntityResource<Project>> projectCache;

    /**
     * Returns the list of route definitions for message production endpoints.
     * Defines a POST route for producing messages to topics.
     *
     * @return List of RouteDefinition objects defining the produce endpoints
     */
    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
            "/v1/projects/:project",
            List.of(
                RouteDefinition.post(Constants.MethodNames.PRODUCE, API_NAME, "/topics/:topic/produce")
                               .hasBody()
                               .nonBlocking()
                               .metricsEnabled()
                               .authorize(ResourceAction.TOPIC_PRODUCE)
                               .build(this::getHierarchies, this::produce)
            )
        ).get();
    }

    /**
     * Gets the resource hierarchies for authorization purposes.
     * Creates a topic hierarchy based on the project and topic from the request path.
     *
     * @param ctx     The routing context containing request information
     * @param hasBody Whether the request has a body
     * @return Map of resource type to resource hierarchy
     */
    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        Project project = projectCache.getOrThrow(ctx.request().getParam(PathParams.PATH_PARAM_PROJECT)).getEntity();
        return Map.of(
            ResourceType.TOPIC,
            new Hierarchies.TopicHierarchy(project, ctx.request().getParam(PathParams.PATH_PARAM_TOPIC))
        );
    }

    /**
     * Handles message production requests to a topic.
     * Processes the request, produces the message, and handles the response asynchronously.
     *
     * <p>The handler:
     * <ul>
     *   <li>Extracts message payload and headers from the request</li>
     *   <li>Builds a message with standard headers</li>
     *   <li>Produces the message to the specified topic</li>
     *   <li>Tracks metrics for the production request</li>
     *   <li>Handles success/failure responses</li>
     * </ul>
     *
     * @param ctx The routing context containing the request and response information
     */
    public void produce(RoutingContext ctx) {
        String projectName = ctx.pathParam(PathParams.PATH_PARAM_PROJECT);
        String topicName = ctx.pathParam(PathParams.PATH_PARAM_TOPIC);
        String topicFQN = VaradhiTopic.fqn(projectName, topicName);

        // FIXME: Optimize memory usage by avoiding buffer copy
        // Current implementation: Uses getBytes() which creates a copy of the entire buffer
        // Potential solution: Use getByteBuf().array() to access the backing array directly,
        // but need to implement proper bounds handling for partial buffer reads
        byte[] payload = ctx.body().buffer().getBytes();
        Message messageToProduce = buildMessageToProduce(payload, ctx.request().headers(), ctx.getIdentityOrDefault());
        CompletableFuture<ProduceResult> result = producerService.produceToTopic(messageToProduce, topicFQN);
        result.whenComplete((produceResult, failure) -> ctx.vertx().runOnContext((Void) -> {
            if (produceResult != null) {
                if (produceResult.isSuccess()) {
                    ctx.endRequestWithResponse(produceResult.getMessageId());
                } else {
                    ctx.endRequestWithStatusAndErrorMsg(
                        getHttpStatusForProduceStatus(produceResult.getProduceStatus()),
                        produceResult.getFailureReason()
                    );
                }
            } else {
                log.error(
                    "produceToTopic({}, {}) failed unexpectedly.",
                    messageToProduce.getMessageId(),
                    topicFQN,
                    failure
                );
                ctx.fail(failure);
            }
        }));
    }

    /**
     * Maps produce status to appropriate HTTP status codes.
     *
     * @param produceStatus The status of the produce operation
     * @return The corresponding HTTP status code
     */
    private int getHttpStatusForProduceStatus(ProduceStatus produceStatus) {
        return switch (produceStatus) {
            case Blocked, NotAllowed -> HttpCodes.HTTP_UNPROCESSABLE_ENTITY;
            case Throttled -> HttpCodes.HTTP_RATE_LIMITED;
            case Failed -> HTTP_INTERNAL_ERROR;
            default -> {
                log.error("Unexpected Produce ProduceStatus ({}) for Http code conversion.", produceStatus);
                yield HTTP_INTERNAL_ERROR;
            }
        };
    }

    /**
     * Builds a message object from the request payload and headers.
     * Validates headers and enriches them with standard system headers.
     *
     * @param payload The message payload as byte array
     * @param headers The request headers
     * @param producerIdentity The identity of the producer
     * @return A Message object ready for production
     */
    Message buildMessageToProduce(byte[] payload, MultiMap headers, String producerIdentity) {
        Multimap<String, String> compliantHeaders = filterCompliantHeaders(headers);
        Message message = new SimpleMessage(payload, compliantHeaders);
        MessageRequestValidator.ensureHeaderSemanticsAndSize(msgConfig, message);
        compliantHeaders.put(StdHeaders.get().produceRegion(), produceRegion);
        compliantHeaders.put(StdHeaders.get().producerIdentity(), producerIdentity);
        compliantHeaders.put(StdHeaders.get().produceTimestamp(), Long.toString(System.currentTimeMillis()));
        return message;
    }

    /**
     * Converting headers to uppercase and filtering non-compliant ones.
     * This step is necessary as Vert.x's MultiMap is already case-insensitive,
     * allowing access to headers in a case-insensitive manner before converting
     * to Google Multimap. The conversion to uppercase ensures consistent case insensitivity in Google Multimap.
     *
     * @param headers
     * @return Multimap with headers converted to uppercase and non-compliant headers filtered
     */
    public Multimap<String, String> filterCompliantHeaders(MultiMap headers) {
        Multimap<String, String> copy = ArrayListMultimap.create();

        boolean filterNonCompliant = msgConfig.isFilterNonCompliantHeaders();
        List<String> allowedPrefixes = msgConfig.getStdHeaders().allowedPrefix();

        for (Map.Entry<String, String> entry : headers) {
            String key = entry.getKey().toUpperCase();
            if (!filterNonCompliant || allowedPrefixes.stream().anyMatch(key::startsWith)) {
                copy.put(key, entry.getValue());
            }
        }
        return copy;
    }
}
