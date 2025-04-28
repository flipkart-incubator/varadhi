package com.flipkart.varadhi.web.v1.produce;

import com.flipkart.varadhi.common.EntityReadCache;
import com.flipkart.varadhi.common.SimpleMessage;
import com.flipkart.varadhi.config.MessageConfiguration;
import com.flipkart.varadhi.entities.Hierarchies;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.ProduceStatus;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.entities.StdHeaders;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.produce.ProducerErrorMapper;
import com.flipkart.varadhi.produce.config.ProducerErrorType;
import com.flipkart.varadhi.produce.otel.ProducerMetricHandler;
import com.flipkart.varadhi.produce.otel.ProducerMetricsEmitter;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.utils.MessageRequestValidator;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.Extensions.RequestBodyExtension;
import com.flipkart.varadhi.web.Extensions.RoutingContextExtension;
import com.flipkart.varadhi.web.metrics.HttpApiMetricsEmitter;
import com.flipkart.varadhi.web.metrics.HttpApiMetricsHandler;
import com.flipkart.varadhi.web.metrics.HttpErrorTypeMapper;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.ext.web.RoutingContext;
import lombok.RequiredArgsConstructor;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.common.Constants.HttpCodes.HTTP_RATE_LIMITED;
import static com.flipkart.varadhi.common.Constants.HttpCodes.HTTP_UNPROCESSABLE_ENTITY;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_TOPIC;
import static com.flipkart.varadhi.common.Constants.Tags.TAG_IDENTITY;
import static com.flipkart.varadhi.common.Constants.Tags.TAG_REGION;
import static com.flipkart.varadhi.common.Constants.Tags.TAG_TOPIC;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_PRODUCE;
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
@ExtensionMethod ({RequestBodyExtension.class, RoutingContextExtension.class, Extensions.class})
@RequiredArgsConstructor
public class ProduceHandlers implements RouteProvider {
    private static final String API_NAME = "TOPIC";
    private final ProducerService producerService;
    private final Handler<RoutingContext> preProduceHandler;
    private final ProducerMetricHandler producerMetricHandler;
    private final HttpApiMetricsHandler httpApiMetricsHandler;
    private final MessageConfiguration msgConfig;
    private final String produceRegion;
    private final EntityReadCache<Project> projectCache;

    private final ThreadLocal<ProducerMetricsEmitter> producerMetricsEmitter = new ThreadLocal<>();
    private final ThreadLocal<HttpApiMetricsEmitter> httpApiMetricsEmitter = new ThreadLocal<>();

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
                RouteDefinition.post("produce", API_NAME, "/topics/:topic/produce")
                               .hasBody()
                               .nonBlocking()
                               .metricsEnabled()
                               .preHandler(preProduceHandler)
                               .authorize(TOPIC_PRODUCE)
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
        Project project = projectCache.getOrThrow(ctx.request().getParam(PATH_PARAM_PROJECT));
        return Map.of(
            ResourceType.TOPIC,
            new Hierarchies.TopicHierarchy(project, ctx.request().getParam(PATH_PARAM_TOPIC))
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
        long requestStartTime = System.currentTimeMillis();
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        String topicName = ctx.pathParam(PATH_PARAM_TOPIC);
        String varadhiTopicName = VaradhiTopic.buildTopicName(projectName, topicName);

        initializeMetricsEmitter(ctx, varadhiTopicName);

        try {
            // FIXME: Optimize memory usage by avoiding buffer copy
            // Current implementation: Uses getBytes() which creates a copy of the entire buffer
            // Potential solution: Use getByteBuf().array() to access the backing array directly,
            // but need to implement proper bounds handling for partial buffer reads
            byte[] payload = ctx.body().buffer().getBytes();
            Message messageToProduce = buildMessageToProduce(payload, ctx.request().headers(), ctx);

            handleProduceToTopic(ctx, requestStartTime, messageToProduce, varadhiTopicName);
        } catch (Exception e) {
            // Record HTTP API error metrics and rethrow
            recordHttpApiError(ctx);
            throw e;
        }
    }

    /**
     * Initializes the metrics emitter for the current request.
     * Sets up the emitter with topic, region, and identity attributes for metric tracking.
     *
     * @param ctx       The routing context containing request information
     * @param topicName The name of the topic being produced to
     */
    private void initializeMetricsEmitter(RoutingContext ctx, String topicName) {
        Map<String, String> produceAttributes = ctx.getRequestAttributes();
        String produceIdentity = ctx.getIdentityOrDefault();

        // FIXME: Fix Attribute Name Semantics here
        produceAttributes.put(TAG_TOPIC, topicName);
        produceAttributes.put(TAG_REGION, produceRegion);
        produceAttributes.put(TAG_IDENTITY, produceIdentity);
        producerMetricsEmitter.set(producerMetricHandler.getEmitter(produceAttributes));

        // Set up HTTP API metrics
        String apiName = "produce";
        httpApiMetricsEmitter.set(httpApiMetricsHandler.getEmitter(apiName, produceAttributes));
    }

    /**
     * Records HTTP API error metrics for an exception.
     *
     * @param ctx The routing context containing the request information
     */
    private void recordHttpApiError(RoutingContext ctx) {
        int statusCode = ctx.response().getStatusCode();
        String errorType = HttpErrorTypeMapper.mapStatusCodeToErrorType(statusCode);

        HttpApiMetricsEmitter emitter = httpApiMetricsEmitter.get();
        if (emitter != null) {
            emitter.recordError(statusCode, errorType);
            httpApiMetricsEmitter.remove();
        }
    }

    /**
     * Handles the asynchronous production of a message to a topic.
     * Creates a CompletableFuture for the produce operation and sets up completion handling.
     *
     * @param ctx              The routing context for the request
     * @param requestStartTime The timestamp when the request started
     * @param messageToProduce The message to be produced
     * @param varadhiTopicName The fully qualified topic name
     */
    private void handleProduceToTopic(
        RoutingContext ctx,
        long requestStartTime,
        Message messageToProduce,
        String varadhiTopicName
    ) {
        CompletableFuture<ProduceResult> produceFuture = producerService.produceToTopic(
            messageToProduce,
            varadhiTopicName,
            producerMetricsEmitter.get()
        );

        produceFuture.whenComplete(
            (produceResult, failure) -> ctx.vertx()
                                           .runOnContext(
                                               v -> handleProduceCompletion(
                                                   ctx,
                                                   requestStartTime,
                                                   messageToProduce,
                                                   varadhiTopicName,
                                                   produceResult,
                                                   failure
                                               )
                                           )
        );
    }

    /**
     * Handles the completion of a produce operation.
     * Emits metrics and processes the result or failure of the produce operation.
     *
     * @param ctx              The routing context for the request
     * @param requestStartTime The timestamp when the request started
     * @param messageToProduce The message that was produced
     * @param varadhiTopicName The fully qualified topic name
     * @param produceResult    The result of the produce operation, null if failed
     * @param failure          The exception if the operation failed, null if successful
     */
    private void handleProduceCompletion(
        RoutingContext ctx,
        long requestStartTime,
        Message messageToProduce,
        String varadhiTopicName,
        ProduceResult produceResult,
        Throwable failure
    ) {
        long totalLatency = System.currentTimeMillis() - requestStartTime;
        emitProducerMetrics(totalLatency, failure);

        try {
            if (produceResult != null) {
                handleProduceResult(ctx, produceResult);
            } else {
                log.error(
                    "produceToTopic({}, {}) failed unexpectedly.",
                    messageToProduce.getMessageId(),
                    varadhiTopicName,
                    failure
                );
                ctx.endRequestWithException(failure);
            }
        } finally {
            // Record HTTP API metrics at the end of processing
            recordHttpApiMetrics(ctx);
        }
    }

    /**
     * Records HTTP API metrics for the completed request.
     *
     * @param ctx The routing context containing response information
     */
    private void recordHttpApiMetrics(RoutingContext ctx) {
        HttpApiMetricsEmitter emitter = httpApiMetricsEmitter.get();
        if (emitter != null) {
            int statusCode = ctx.response().getStatusCode();

            if (statusCode >= 200 && statusCode < 400) {
                emitter.recordSuccess(statusCode);
            } else {
                String errorType = HttpErrorTypeMapper.mapStatusCodeToErrorType(statusCode);
                emitter.recordError(statusCode, errorType);
            }

            httpApiMetricsEmitter.remove();
        }
    }

    /**
     * Emits metrics for the produce operation.
     * Records latency and error information using the metrics emitter.
     *
     * @param totalLatency The total time taken for the produce operation
     * @param failure      The exception if the operation failed, null if successful
     */
    private void emitProducerMetrics(long totalLatency, Throwable failure) {
        producerMetricsEmitter.get()
                              .emit(
                                  true,
                                  totalLatency,
                                  0,
                                  0,
                                  false,
                                  failure != null ? ProducerErrorMapper.mapToProducerErrorType(failure) : null
                              );
    }

    /**
     * Handles the result of a successful produce operation.
     * Sends appropriate response based on the produce status.
     *
     * @param ctx           The routing context for the request
     * @param produceResult The result of the produce operation
     */
    private void handleProduceResult(RoutingContext ctx, ProduceResult produceResult) {
        if (produceResult.isSuccess()) {
            ctx.endRequestWithResponse(produceResult.getMessageId());
        } else {
            ctx.endRequestWithStatusAndErrorMsg(
                getHttpStatusForProduceStatus(produceResult.getProduceStatus()),
                produceResult.getFailureReason()
            );
        }
    }

    /**
     * Maps produce status to appropriate HTTP status codes.
     *
     * @param produceStatus The status of the produce operation
     * @return The corresponding HTTP status code
     */
    private int getHttpStatusForProduceStatus(ProduceStatus produceStatus) {
        return switch (produceStatus) {
            case Blocked, NotAllowed -> HTTP_UNPROCESSABLE_ENTITY;
            case Throttled -> HTTP_RATE_LIMITED;
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
     * @param ctx     The routing context
     * @return A Message object ready for production
     */
    private Message buildMessageToProduce(byte[] payload, MultiMap headers, RoutingContext ctx) {
        try {
            Multimap<String, String> compliantHeaders = filterCompliantHeaders(headers);
            MessageRequestValidator.ensureHeaderSemanticsAndSize(msgConfig, compliantHeaders, payload.length);

            enrichHeaders(compliantHeaders, ctx.getIdentityOrDefault());
            return new SimpleMessage(payload, compliantHeaders);
        } catch (Exception e) {
            producerMetricsEmitter.get().emit(false, 0, 0, 0, false, ProducerErrorType.SERIALIZE);
            throw e;
        }
    }

    /**
     * Enriches message headers with standard system headers.
     * Adds produce region, identity, and timestamp headers to the message.
     *
     * @param headers         The message headers to enrich
     * @param produceIdentity The identity of the producer
     */
    private void enrichHeaders(Multimap<String, String> headers, String produceIdentity) {
        StdHeaders stdHeaders = StdHeaders.get();
        headers.put(stdHeaders.produceRegion(), produceRegion);
        headers.put(stdHeaders.produceIdentity(), produceIdentity);
        headers.put(stdHeaders.produceTimestamp(), Long.toString(System.currentTimeMillis()));
    }

    /**
     * Filters and converts headers to a compliant format.
     * Converts headers to uppercase and filters based on allowed prefixes if configured.
     *
     * <p>This conversion ensures consistent case-insensitive header handling when converting
     * from Vert.x MultiMap to Google Multimap.</p>
     *
     * @param headers The original headers from the request
     * @return A Multimap containing the filtered and converted headers
     */
    public Multimap<String, String> filterCompliantHeaders(MultiMap headers) {
        Multimap<String, String> copy = ArrayListMultimap.create();
        boolean filterNonCompliant = msgConfig.isFilterNonCompliantHeaders();
        List<String> allowedPrefixes = msgConfig.getStdHeaders().allowedPrefix();

        headers.entries()
               .stream()
               .filter(
                   entry -> !filterNonCompliant || allowedPrefixes.stream()
                                                                  .anyMatch(entry.getKey().toUpperCase()::startsWith)
               )
               .forEach(entry -> copy.put(entry.getKey().toUpperCase(), entry.getValue()));

        return copy;
    }
}
