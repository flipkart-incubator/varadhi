package com.flipkart.varadhi.web.v1.produce;

import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.services.ProducerService;
import com.flipkart.varadhi.web.Extensions.RequestBodyExtension;
import com.flipkart.varadhi.web.Extensions.RoutingContextExtension;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

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
                                Optional.of(PermissionAuthorization.of(TOPIC_PRODUCE, "{project}/{topic}"))
                        )
                )
        ).get();
    }

    public void produce(RoutingContext ctx) {
        ProduceContext produceContext = new ProduceContext(ctx, this.deployedRegion);
        MessageResource messageResource = ctx.body().asPojo(MessageResource.class);
        String projectName = ctx.pathParam("project");
        String topicName = ctx.pathParam("topic");
        String varadhiTopicName = VaradhiTopic.getTopicFQN(projectName, topicName);
        Message message = messageResource.getMessageToProduce();
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
}
