package com.flipkart.varadhi.web.v1.produce;

import com.flipkart.varadhi.MessageConstants;
import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.produce.services.ProducerService;
import com.flipkart.varadhi.web.Extensions.RequestBodyExtension;
import com.flipkart.varadhi.web.Extensions.RoutingContextExtension;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.Constants.REQUEST_PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.Constants.REQUEST_PATH_PARAM_TOPIC;
import static com.flipkart.varadhi.auth.ResourceAction.TOPIC_PRODUCE;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.authenticated;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.hasBody;


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
                                HttpMethod.POST, "/topics/:topic/produce", Set.of(authenticated, hasBody),
                                this::produce,
                                false,
                                Optional.of(PermissionAuthorization.of(TOPIC_PRODUCE, "{project}/{topic}"))
                        )
                )
        ).get();
    }

    public void produce(RoutingContext ctx) {
        //TODO:: Request Validations pending
        // Also close on what happens if fields are missing (like msgId) or groupId.
        MessageResource messageResource = ctx.body().asPojo(MessageResource.class);
        String projectName = ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
        String topicName = ctx.pathParam(REQUEST_PATH_PARAM_TOPIC);
        String varadhiTopicName = VaradhiTopic.getTopicFQN(projectName, topicName);
        Message message = messageResource.getMessageToProduce();
        ProduceContext produceContext = buildProduceContext(ctx);
        CompletableFuture<ProduceResult> produceFuture =
                this.producerService.produceToTopic(message, varadhiTopicName, produceContext);
        produceFuture.whenComplete((produceresult, failure) -> {
            //TODO::log/metric.
            ctx.vertx().runOnContext((Void) -> {
                        if (null != failure) {
                            ctx.endRequestWithException(failure);
                        } else {
                            ctx.endRequestWithResponse(produceresult.getProduceRestResponse());
                        }
                    }
            );
        });
    }

    private ProduceContext buildProduceContext(RoutingContext ctx) {
        UserContext userContext = buildUserContext(ctx.user());
        ProduceContext.RequestContext requestContext = buildRequestContext(ctx.request());
        ProduceContext.TopicContext topicContext = buildTopicContext(ctx.request(), this.deployedRegion);
        return null;
    }


    private UserContext buildUserContext(User user) {
        return user == null ? null : new VertxUserContext(user);
    }

    private ProduceContext.RequestContext buildRequestContext(HttpServerRequest request) {
        ProduceContext.RequestContext requestContext = new ProduceContext.RequestContext();
        requestContext.setRequestPath(request.path());
        requestContext.setAbsoluteUri(request.absoluteURI());
        requestContext.setRequestTimestamp(System.currentTimeMillis());
        request.headers().forEach((key, value) -> requestContext.getHeaders().put(key, value));
        requestContext.setBytesReceived(request.bytesRead());
        requestContext.setHttpMethod(request.method());
        String remoteHost = request.remoteAddress().host();
        String xfwdedHeaderValue = request.getHeader(MessageConstants.HEADER_X_FWDED_FOR);
        if (null != xfwdedHeaderValue && !xfwdedHeaderValue.isEmpty()) {
            // This could be multivalued (comma separated), take the original initiator i.e. left most in the list.
            String[] proxies = xfwdedHeaderValue.trim().split(",");
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
