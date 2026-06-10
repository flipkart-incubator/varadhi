package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.cluster.controller.ControllerApi;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverRequest;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverTransition;
import com.flipkart.varadhi.web.Extensions;
import com.flipkart.varadhi.web.hierarchy.Hierarchies.ProjectHierarchy;
import com.flipkart.varadhi.web.hierarchy.Hierarchies.TopicHierarchy;
import com.flipkart.varadhi.web.hierarchy.ResourceHierarchy;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_TOPIC;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_GET;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_UPDATE;

/**
 * REST routes for topic failover:
 *
 * <pre>
 *   POST   /v1/projects/:project/topics/:topic/failover         -> create
 *   GET    /v1/projects/:project/topics/:topic/failover         -> read snapshot
 *   POST   /v1/projects/:project/topics/:topic/failover/abort   -> abort (pre-SWITCH only)
 *   GET    /v1/admin/failovers/active                           -> list all live
 * </pre>
 *
 * <p>All routes round-trip through {@link ControllerApi} (typically the {@code
 * ControllerRestClient} on the webserver, which talks to the controller via the
 * Vert.x event bus); the controller side enforces business invariants and
 * persistence.
 */
@Slf4j
public final class TopicFailoverHandlers implements RouteProvider {

    private static final String API_NAME = "TOPIC_FAILOVER";
    private static final String TOPIC_BASE_PATH = "/v1/projects/:project/topics";
    private static final String ADMIN_PATH = "/v1/admin/failovers";

    private final ControllerApi controllerApi;
    private final ResourceReadCache<Resource.EntityResource<Project>> projectCache;

    public TopicFailoverHandlers(
        ControllerApi controllerApi,
        ResourceReadCache<Resource.EntityResource<Project>> projectCache
    ) {
        this.controllerApi = controllerApi;
        this.projectCache = projectCache;
    }

    @Override
    public List<RouteDefinition> get() {
        List<RouteDefinition> topicRoutes = new SubRoutes(
            TOPIC_BASE_PATH,
            List.of(
                RouteDefinition.post("createFailover", API_NAME, "/:topic/failover")
                               .hasBody()
                               .nonBlocking()
                               .authorize(TOPIC_UPDATE)
                               .build(this::topicHierarchies, this::create),
                RouteDefinition.get("getFailover", API_NAME, "/:topic/failover")
                               .nonBlocking()
                               .authorize(TOPIC_GET)
                               .build(this::topicHierarchies, this::read),
                RouteDefinition.post("abortFailover", API_NAME, "/:topic/failover/abort")
                               .nonBlocking()
                               .authorize(TOPIC_UPDATE)
                               .build(this::topicHierarchies, this::abort)
            )
        ).get();
        List<RouteDefinition> adminRoutes = new SubRoutes(
            ADMIN_PATH,
            List.of(
                // Admin endpoint isn't scoped to a single topic, so the path param-less variant.
                RouteDefinition.get("listActiveFailovers", API_NAME, "/active")
                               .nonBlocking()
                               // Admin: TOPIC_UPDATE is a coarse-but-existing auth scope. Replace with a
                               // dedicated FAILOVER_ADMIN action once we add it to ResourceAction.
                               .authorize(TOPIC_UPDATE)
                               .build(this::rootHierarchies, this::listActive)
            )
        ).get();
        return java.util.stream.Stream.concat(topicRoutes.stream(), adminRoutes.stream()).toList();
    }

    public void create(RoutingContext ctx) {
        String topicFqn = topicFqn(ctx);
        TopicFailoverRequest body = ctx.body().asPojo(TopicFailoverRequest.class);
        if (body == null || body.getToRegion() == null) {
            ctx.fail(400, new IllegalArgumentException("toRegion is required in the request body"));
            return;
        }
        String requestedBy = Extensions.RoutingContextExtension.getIdentityOrDefault(ctx);
        Extensions.RoutingContextExtension.handleResponse(
            ctx,
            controllerApi.createTopicFailover(topicFqn, body, requestedBy)
        );
    }

    public void read(RoutingContext ctx) {
        String topicFqn = topicFqn(ctx);
        controllerApi.getTopicFailover(topicFqn).whenComplete((opt, t) -> ctx.vertx().runOnContext(v -> {
            if (t != null) {
                ctx.fail(t);
                return;
            }
            TopicFailoverTransition value = opt.orElse(null);
            if (value == null) {
                ctx.fail(404, new ResourceNotFoundException("No active failover for topic " + topicFqn));
            } else {
                Extensions.RoutingContextExtension.endRequestWithResponse(ctx, value);
            }
        }));
    }

    public void abort(RoutingContext ctx) {
        String topicFqn = topicFqn(ctx);
        String requestedBy = Extensions.RoutingContextExtension.getIdentityOrDefault(ctx);
        Extensions.RoutingContextExtension.handleResponse(
            ctx,
            controllerApi.abortTopicFailover(topicFqn, requestedBy)
        );
    }

    public void listActive(RoutingContext ctx) {
        Extensions.RoutingContextExtension.handleResponse(ctx, controllerApi.listActiveTopicFailovers());
    }

    private Map<ResourceType, ResourceHierarchy> topicHierarchies(RoutingContext ctx, boolean hasBody) {
        String projectName = ctx.request().getParam(PATH_PARAM_PROJECT);
        Project project = projectCache.getOrThrow(projectName).getEntity();
        String topicName = ctx.request().getParam(PATH_PARAM_TOPIC);
        if (topicName == null) {
            return Map.of(ResourceType.PROJECT, new ProjectHierarchy(project));
        }
        return Map.of(ResourceType.TOPIC, new TopicHierarchy(project, topicName));
    }

    private Map<ResourceType, ResourceHierarchy> rootHierarchies(RoutingContext ctx, boolean hasBody) {
        // No project context; authz uses the ROOT hierarchy (same convention as ORG_LIST).
        return Map.of();
    }

    private String topicFqn(RoutingContext ctx) {
        return VaradhiTopicName.of(ctx.pathParam(PATH_PARAM_PROJECT), ctx.pathParam(PATH_PARAM_TOPIC)).toFqn();
    }
}
