package com.flipkart.varadhi.web.v1.produce;

import com.flipkart.varadhi.config.RestOptions;
import com.flipkart.varadhi.core.entities.ApiContext;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.utils.HeaderUtils;
import com.flipkart.varadhi.utils.MessageHelper;
import com.flipkart.varadhi.web.Extensions.RequestBodyExtension;
import com.flipkart.varadhi.web.Extensions.RoutingContextExtension;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import com.google.common.collect.Multimap;
import io.vertx.core.MultiMap;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.Constants.API_CONTEXT_KEY;
import static com.flipkart.varadhi.Constants.HttpCodes.HTTP_RATE_LIMITED;
import static com.flipkart.varadhi.Constants.HttpCodes.HTTP_UNPROCESSABLE_ENTITY;
import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_TOPIC;
import static com.flipkart.varadhi.entities.StandardHeaders.*;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_PRODUCE;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;


@Slf4j
@ExtensionMethod({RequestBodyExtension.class, RoutingContextExtension.class})
public class ProduceHandlers implements RouteProvider {
    private final ProducerService producerService;
    private final HeaderValidationHandler headerValidationHandler;

    public ProduceHandlers(RestOptions restOptions, ProducerService producerService) {
        this.producerService = producerService;
        this.headerValidationHandler = new HeaderValidationHandler(restOptions);
    }

    @Override
    public List<RouteDefinition> get() {

        return new SubRoutes(
                "/v1/projects/:project",
                List.of(
                        RouteDefinition.post("Produce", "/topics/:topic/produce")
                                .hasBody()
                                .nonBlocking()
                                .preHandler(headerValidationHandler::validate)
                                .authorize(TOPIC_PRODUCE, "{project}/{topic}")
                                .build(this::produce)
                )
        ).get();
    }

    public void produce(RoutingContext ctx) {

        String projectName = ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
        String topicName = ctx.pathParam(REQUEST_PATH_PARAM_TOPIC);

        String varadhiTopicName = VaradhiTopic.buildTopicName(projectName, topicName);

        // TODO:: Below is making extra copy, this needs to be avoided.
        // ctx.body().buffer().getByteBuf().array() -- method gives complete backing array w/o copy,
        // however only required bytes are needed. Need to figure out the correct mechanism here.
        byte[] payload = ctx.body().buffer().getBytes();
        ApiContext apiContext = ctx.get(API_CONTEXT_KEY);
        Message messageToProduce = buildMessageToProduce(payload, ctx.request().headers(), apiContext);
        CompletableFuture<ProduceResult> produceFuture =
                producerService.produceToTopic(messageToProduce, varadhiTopicName, apiContext);
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
            ApiContext apiContext
    ) {
        Multimap<String, String> requestHeaders = HeaderUtils.copyVaradhiHeaders(headers);
        requestHeaders.put(PRODUCE_TIMESTAMP, Long.toString(apiContext.get(ApiContext.START_TIME)));
        requestHeaders.put(PRODUCE_IDENTITY, apiContext.get(ApiContext.IDENTITY));
        requestHeaders.put(PRODUCE_REGION, apiContext.get(ApiContext.REGION));
        MessageHelper.ensureRequiredHeaders(requestHeaders);
        return new Message(payload, requestHeaders);
    }
}
