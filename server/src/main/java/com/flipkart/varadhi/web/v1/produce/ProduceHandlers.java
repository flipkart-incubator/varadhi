package com.flipkart.varadhi.web.v1.produce;

import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.ProduceContext;
import com.flipkart.varadhi.entities.ProduceResult;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.utils.HeaderUtils;
import com.flipkart.varadhi.web.Extensions.RequestBodyExtension;
import com.flipkart.varadhi.web.Extensions.RoutingContextExtension;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import com.google.common.collect.Multimap;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.Constants.HttpCodes.HTTP_RATE_LIMITED;
import static com.flipkart.varadhi.Constants.HttpCodes.HTTP_UNPROCESSABLE_ENTITY;
import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_TOPIC;
import static com.flipkart.varadhi.MessageConstants.ANONYMOUS_PRODUCE_IDENTITY;
import static com.flipkart.varadhi.MessageConstants.Headers.*;
import static com.flipkart.varadhi.MessageConstants.PRODUCE_CHANNEL_HTTP;
import static com.flipkart.varadhi.auth.ResourceAction.TOPIC_PRODUCE;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.authenticated;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.hasBody;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;


@Slf4j
@ExtensionMethod({RequestBodyExtension.class, RoutingContextExtension.class})
public class ProduceHandlers implements RouteProvider {
    private final String deployedRegion;
    private final ProducerService producerService;

    public ProduceHandlers(String deployedRegion, ProducerService producerService) {
        this.deployedRegion = deployedRegion;
        this.producerService = producerService;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
                "/v1/projects/:project",
                List.of(
                        new RouteDefinition(
                                HttpMethod.POST,
                                "/topics/:topic/produce",
                                Set.of(authenticated, hasBody),
                                new LinkedHashSet(),
                                this::produce,
                                false,
                                Optional.of(PermissionAuthorization.of(TOPIC_PRODUCE, "{project}/{topic}"))
                        )
                )
        ).get();
    }

    public void produce(RoutingContext ctx) {
        //TODO:: Request Validations pending

        byte[] payload = ctx.body().asPojo(byte[].class);

        String projectName = ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
        String topicName = ctx.pathParam(REQUEST_PATH_PARAM_TOPIC);
        String varadhiTopicName = VaradhiTopic.buildTopicName(projectName, topicName);

        ProduceContext produceContext = buildProduceContext(ctx, payload.length);
        Message messageToProduce = buildMessageToProduce(payload, ctx.request().headers(), produceContext);

        CompletableFuture<ProduceResult> produceFuture =
                producerService.produceToTopic(messageToProduce, varadhiTopicName, produceContext);
        produceFuture.whenComplete((produceResult, failure) ->
                ctx.vertx().runOnContext((Void) -> {
                            if (null != produceResult) {
                                if (produceResult.isSuccess()) {
                                    ctx.endRequestWithResponse(produceResult.getMessageId());
                                } else {
                                    ctx.endRequestWithStatusAndErrorMsg(
                                            getHttpStatusForProduceStatus(produceResult.getProduceStatus().status()),
                                            produceResult.getProduceStatus().message()
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

    private int getHttpStatusForProduceStatus(ProduceResult.Status status) {
        return switch (status) {
            case Blocked, NotAllowed -> HTTP_UNPROCESSABLE_ENTITY;
            case Throttled -> HTTP_RATE_LIMITED;
            case Failed -> HTTP_INTERNAL_ERROR;
            default -> {
                log.error("Unexpected Produce Status ({}) for Http code conversion.", status);
                yield HTTP_INTERNAL_ERROR;
            }
        };
    }


    private Message buildMessageToProduce(
            byte[] payload,
            MultiMap headers,
            ProduceContext produceContext
    ) {
        Multimap<String, String> requestHeaders = HeaderUtils.copyVaradhiHeaders(headers);
        requestHeaders.put(PRODUCE_TIMESTAMP, Long.toString(produceContext.getRequestContext().getRequestTimestamp()));
        requestHeaders.put(PRODUCE_IDENTITY, produceContext.getRequestContext().getProduceIdentity());
        requestHeaders.put(PRODUCE_REGION, produceContext.getTopicContext().getRegion());
        return new Message(payload, requestHeaders);
    }

    private ProduceContext buildProduceContext(RoutingContext ctx, int payloadSize) {
        ProduceContext.RequestContext requestContext = buildRequestContext(ctx, payloadSize);
        ProduceContext.TopicContext topicContext = buildTopicContext(ctx.request(), this.deployedRegion);
        return new ProduceContext(requestContext, topicContext);
    }

    private ProduceContext.RequestContext buildRequestContext(RoutingContext ctx, int payloadSize) {
        HttpServerRequest request = ctx.request();
        ProduceContext.RequestContext requestContext = new ProduceContext.RequestContext();
        String produceIdentity = ctx.user() == null ? ANONYMOUS_PRODUCE_IDENTITY : ctx.user().subject();
        requestContext.setProduceIdentity(produceIdentity);
        requestContext.setRequestTimestamp(System.currentTimeMillis());
        requestContext.setBytesReceived(payloadSize);
        requestContext.setRequestChannel(PRODUCE_CHANNEL_HTTP);
        String remoteHost = request.remoteAddress().host();
        String xForwardedForValue = request.getHeader(FORWARDED_FOR);
        if (null != xForwardedForValue && !xForwardedForValue.isEmpty()) {
            // This could be multivalued (comma separated), take the original initiator i.e. left most in the list.
            String[] proxies = xForwardedForValue.trim().split(",");
            if (!proxies[0].isBlank()) {
                remoteHost = proxies[0];
            }
        }
        requestContext.setRemoteHost(remoteHost);
        return requestContext;
    }

    private ProduceContext.TopicContext buildTopicContext(HttpServerRequest request, String produceRegion) {
        ProduceContext.TopicContext topicContext = new ProduceContext.TopicContext();
        topicContext.setRegion(produceRegion);
        topicContext.setTopicName(request.getParam(REQUEST_PATH_PARAM_TOPIC));
        topicContext.setProjectName(request.getParam(REQUEST_PATH_PARAM_PROJECT));
        return topicContext;
    }
}
