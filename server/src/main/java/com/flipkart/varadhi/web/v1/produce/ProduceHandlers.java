package com.flipkart.varadhi.web.v1.produce;

import static com.flipkart.varadhi.common.Constants.HttpCodes.HTTP_RATE_LIMITED;
import static com.flipkart.varadhi.common.Constants.HttpCodes.HTTP_UNPROCESSABLE_ENTITY;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_TOPIC;
import static com.flipkart.varadhi.common.Constants.Tags.TAG_IDENTITY;
import static com.flipkart.varadhi.common.Constants.Tags.TAG_REGION;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_PRODUCE;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.flipkart.varadhi.common.SimpleMessage;
import com.flipkart.varadhi.utils.MessageRequestValidator;
import com.flipkart.varadhi.config.MessageConfiguration;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.produce.otel.ProducerMetricHandler;
import com.flipkart.varadhi.produce.otel.ProducerMetricsEmitter;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.Extensions.RequestBodyExtension;
import com.flipkart.varadhi.web.Extensions.RoutingContextExtension;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.ext.web.RoutingContext;
import lombok.AllArgsConstructor;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@ExtensionMethod ({RequestBodyExtension.class, RoutingContextExtension.class, Extensions.class})
@AllArgsConstructor
public class ProduceHandlers implements RouteProvider {
    private final ProducerService producerService;
    private final Handler<RoutingContext> preProduceHandler;
    private final ProjectService projectService;
    private final ProducerMetricHandler metricHandler;
    private final MessageConfiguration msgConfig;
    private final String produceRegion;

    @Override
    public List<RouteDefinition> get() {

        return new SubRoutes(
            "/v1/projects/:project",
            List.of(
                RouteDefinition.post("Produce", "/topics/:topic/produce")
                               .hasBody()
                               .nonBlocking()
                               .metricsEnabled()
                               .preHandler(preProduceHandler)
                               .authorize(TOPIC_PRODUCE)
                               .build(this::getHierarchies, this::produce)
            )
        ).get();
    }

    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        Project project = projectService.getCachedProject(ctx.request().getParam(PATH_PARAM_PROJECT));
        return Map.of(
            ResourceType.TOPIC,
            new Hierarchies.TopicHierarchy(project, ctx.request().getParam(PATH_PARAM_TOPIC))
        );
    }

    public void produce(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        String topicName = ctx.pathParam(PATH_PARAM_TOPIC);

        Map<String, String> produceAttributes = ctx.getRequestAttributes();
        //TODO FIx attribute name semantics here.
        String produceIdentity = ctx.getIdentityOrDefault();
        produceAttributes.put(TAG_REGION, produceRegion);
        produceAttributes.put(TAG_IDENTITY, produceIdentity);
        ProducerMetricsEmitter metricsEmitter = metricHandler.getEmitter(ctx.body().length(), produceAttributes);

        String varadhiTopicName = VaradhiTopic.buildTopicName(projectName, topicName);

        // TODO:: Below is making extra copy, this needs to be avoided.
        // ctx.body().buffer().getByteBuf().array() -- method gives complete backing array w/o copy,
        // however only required bytes are needed. Need to figure out the correct mechanism here.
        byte[] payload = ctx.body().buffer().getBytes();
        Message messageToProduce = buildMessageToProduce(payload, ctx.request().headers(), ctx);
        CompletableFuture<ProduceResult> produceFuture = producerService.produceToTopic(
            messageToProduce,
            varadhiTopicName,
            metricsEmitter
        );
        produceFuture.whenComplete((produceResult, failure) -> ctx.vertx().runOnContext((Void) -> {
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
        }));
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

    private Message buildMessageToProduce(byte[] payload, MultiMap headers, RoutingContext ctx) {
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        headers.forEach(varadhiHeaders::put);
        Multimap<String, String> compliantHeaders = msgConfig.filterCompliantHeaders(varadhiHeaders);
        MessageRequestValidator.ensureHeaderSemanticsAndSize(msgConfig, compliantHeaders, payload.length);
        //enriching headerNames with custom headerNames
        String produceIdentity = ctx.getIdentityOrDefault();

        compliantHeaders.put(StdHeaders.get().produceRegion(), produceRegion);
        compliantHeaders.put(StdHeaders.get().produceIdentity(), produceIdentity);
        compliantHeaders.put(StdHeaders.get().produceTimestamp(), Long.toString(System.currentTimeMillis()));
        return new SimpleMessage(payload, compliantHeaders);
    }
}
