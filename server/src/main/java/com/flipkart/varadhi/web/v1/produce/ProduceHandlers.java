package com.flipkart.varadhi.web.v1.produce;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.produce.otel.ProducerMetricHandler;
import com.flipkart.varadhi.produce.otel.ProducerMetricsEmitter;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.utils.HeaderUtils;
import com.flipkart.varadhi.utils.MessageHelper;
import com.flipkart.varadhi.web.Extensions.RequestBodyExtension;
import com.flipkart.varadhi.web.Extensions.RoutingContextExtension;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import com.google.common.collect.Multimap;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.Constants.CONTEXT_KEY_RESOURCE_HIERARCHY;
import static com.flipkart.varadhi.Constants.HttpCodes.HTTP_RATE_LIMITED;
import static com.flipkart.varadhi.Constants.HttpCodes.HTTP_UNPROCESSABLE_ENTITY;
import static com.flipkart.varadhi.Constants.PathParams.PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.Constants.PathParams.PATH_PARAM_TOPIC;
import static com.flipkart.varadhi.Constants.Tags.TAG_IDENTITY;
import static com.flipkart.varadhi.Constants.Tags.TAG_REGION;
import static com.flipkart.varadhi.MessageConstants.ANONYMOUS_IDENTITY;
import static com.flipkart.varadhi.entities.StandardHeaders.*;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_PRODUCE;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;


@Slf4j
@ExtensionMethod({RequestBodyExtension.class, RoutingContextExtension.class})
public class ProduceHandlers implements RouteProvider {
    private final ProducerService producerService;
    private final Handler<RoutingContext> headerValidationHandler;
    private final ProjectService projectService;
    private final ProducerMetricHandler metricHandler;
    private final String produceRegion;

    public ProduceHandlers(
            String produceRegion, Handler<RoutingContext> headerValidationHandler, ProducerService producerService,
            ProjectService projectService, ProducerMetricHandler metricHandler
    ) {
        this.produceRegion = produceRegion;
        this.producerService = producerService;
        this.headerValidationHandler = headerValidationHandler;
        this.projectService = projectService;
        this.metricHandler = metricHandler;
    }

    @Override
    public List<RouteDefinition> get() {

        return new SubRoutes(
                "/v1/projects/:project",
                List.of(
                        RouteDefinition.post("Produce", "/topics/:topic/produce")
                                .hasBody()
                                .nonBlocking()
                                .metricsEnabled()
                                .preHandler(headerValidationHandler)
                                .authorize(TOPIC_PRODUCE)
                                .build(this::getHierarchy, this::produce)
                )
        ).get();
    }

    public ResourceHierarchy getHierarchy(RoutingContext ctx, boolean hasBody) {
        String projectName = ctx.request().getParam(PATH_PARAM_PROJECT);
        String topicName = ctx.request().getParam(PATH_PARAM_TOPIC);
        Project project = projectService.getCachedProject(projectName);
        return new Hierarchies.TopicHierarchy(project.getOrg(), project.getTeam(), project.getName(), topicName);
    }

    public void produce(RoutingContext ctx) {

        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        String topicName = ctx.pathParam(PATH_PARAM_TOPIC);
        Hierarchies.TopicHierarchy topicHierarchy = ctx.get(CONTEXT_KEY_RESOURCE_HIERARCHY);

        Map<String, String> produceAttributes = topicHierarchy.getAttributes();
        //TODO FIx attribute name semantics here.
        String produceIdentity = ctx.user() == null ? ANONYMOUS_IDENTITY : ctx.user().subject();
        produceAttributes.put(TAG_REGION, produceRegion);
        produceAttributes.put(TAG_IDENTITY, produceIdentity);
        ProducerMetricsEmitter metricsEmitter = metricHandler.getEmitter(ctx.body().length(), produceAttributes);

        String varadhiTopicName = VaradhiTopic.buildTopicName(projectName, topicName);

        // TODO:: Below is making extra copy, this needs to be avoided.
        // ctx.body().buffer().getByteBuf().array() -- method gives complete backing array w/o copy,
        // however only required bytes are needed. Need to figure out the correct mechanism here.
        byte[] payload = ctx.body().buffer().getBytes();
        Message messageToProduce = buildMessageToProduce(payload, ctx.request().headers(), produceIdentity);
        CompletableFuture<ProduceResult> produceFuture =
                producerService.produceToTopic(messageToProduce, varadhiTopicName, metricsEmitter);
        produceFuture.whenComplete((produceResult, failure) ->
                ctx.vertx().runOnContext((Void) -> {
                            if (null != produceResult) {
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
                                        String.format(
                                                "produceToTopic(%s, %s) failed unexpectedly.",
                                                messageToProduce.getMessageId(),
                                                varadhiTopicName
                                        ),
                                        failure
                                );
                                ctx.endRequestWithException(failure);
                            }
                        }
                )
        );
    }

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


    private Message buildMessageToProduce(
            byte[] payload,
            MultiMap headers,
            String produceIdentity
    ) {
        Multimap<String, String> requestHeaders = HeaderUtils.copyVaradhiHeaders(headers);
        requestHeaders.put(PRODUCE_TIMESTAMP, Long.toString(System.currentTimeMillis()));
        requestHeaders.put(PRODUCE_IDENTITY, produceIdentity);
        requestHeaders.put(PRODUCE_REGION, produceRegion);
        MessageHelper.ensureRequiredHeaders(requestHeaders);
        return new Message(payload, requestHeaders);
    }
}
