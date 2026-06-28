package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.cluster.controller.ControllerApi;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverRequest;
import com.flipkart.varadhi.web.Extensions.RequestBodyExtension;
import com.flipkart.varadhi.web.Extensions.RoutingContextExtension;
import com.flipkart.varadhi.web.hierarchy.Hierarchies.TopicHierarchy;
import com.flipkart.varadhi.web.hierarchy.ResourceHierarchy;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

import static com.flipkart.varadhi.common.Constants.ContextKeys.REQUEST_BODY;
import static com.flipkart.varadhi.common.Constants.MethodNames.GET;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_TOPIC;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_GET;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_UPDATE;

/**
 * REST handlers for the topic-failover lifecycle. Each endpoint forwards to the controller (which
 * owns orchestration) over the cluster bus via {@link ControllerApi}.
 *
 * <ul>
 *   <li>POST {@code /v1/projects/:project/topics/:topic/failover} — request a failover
 *       (source→target). Returns {@code 200} with the intent op once durably recorded.</li>
 *   <li>GET  {@code /v1/projects/:project/topics/:topic/failover} — current transition snapshot
 *       (404 if none active).</li>
 *   <li>POST {@code /v1/projects/:project/topics/:topic/failover/abort} — abort, honored only while
 *       the transition is still abortable (PENDING/PREPARE).</li>
 * </ul>
 */
@Slf4j
@ExtensionMethod ({RequestBodyExtension.class, RoutingContextExtension.class})
public class TopicFailoverHandlers implements RouteProvider {

    private static final String API_NAME = "TOPIC_FAILOVER";

    private final ControllerApi controllerClient;
    private final ResourceReadCache<Resource.EntityResource<Project>> projectCache;

    public TopicFailoverHandlers(
        ControllerApi controllerClient,
        ResourceReadCache<Resource.EntityResource<Project>> projectCache
    ) {
        this.controllerClient = controllerClient;
        this.projectCache = projectCache;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
            "/v1/projects/:project/topics",
            List.of(
                RouteDefinition.post("FAILOVER", API_NAME, "/:topic/failover")
                               .hasBody()
                               .bodyParser(this::setRequestBody)
                               .nonBlocking()
                               .authorize(TOPIC_UPDATE)
                               .build(this::getHierarchies, this::create),
                RouteDefinition.get(GET, API_NAME, "/:topic/failover")
                               .nonBlocking()
                               .authorize(TOPIC_GET)
                               .build(this::getHierarchies, this::status),
                RouteDefinition.post("FAILOVER_ABORT", API_NAME, "/:topic/failover/abort")
                               .nonBlocking()
                               .authorize(TOPIC_UPDATE)
                               .build(this::getHierarchies, this::abort)
            )
        ).get();
    }

    public void setRequestBody(RoutingContext ctx) {
        ctx.put(REQUEST_BODY, ctx.body().asPojo(TopicFailoverRequest.class));
    }

    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        String projectName = ctx.request().getParam(PATH_PARAM_PROJECT);
        Project project = projectCache.getOrThrow(projectName).getEntity();
        String topicName = ctx.request().getParam(PATH_PARAM_TOPIC);
        return Map.of(ResourceType.TOPIC, new TopicHierarchy(project, topicName));
    }

    public void create(RoutingContext ctx) {
        TopicFailoverRequest body = ctx.get(REQUEST_BODY);
        TopicFailoverRequest request = body.withRequestedBy(ctx.getIdentityOrDefault());
        ctx.handleResponse(controllerClient.createTopicFailover(getVaradhiTopicName(ctx), request));
    }

    public void status(RoutingContext ctx) {
        ctx.handleResponse(controllerClient.getTopicFailover(getVaradhiTopicName(ctx)));
    }

    public void abort(RoutingContext ctx) {
        ctx.handleResponse(controllerClient.abortTopicFailover(getVaradhiTopicName(ctx), ctx.getIdentityOrDefault()));
    }

    private String getVaradhiTopicName(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        String topicName = ctx.pathParam(PATH_PARAM_TOPIC);
        return VaradhiTopicName.of(projectName, topicName).toFqn();
    }
}
