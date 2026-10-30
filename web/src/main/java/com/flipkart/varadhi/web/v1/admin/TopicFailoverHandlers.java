package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.core.RegionService;
import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.VaradhiTopicService;
import com.flipkart.varadhi.core.cluster.controller.ControllerApi;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.transition.failover.TopicFailoverRequest;
import com.flipkart.varadhi.web.Extensions.RequestBodyExtension;
import com.flipkart.varadhi.web.Extensions.RoutingContextExtension;
import com.flipkart.varadhi.web.hierarchy.Hierarchies.ProjectHierarchy;
import com.flipkart.varadhi.web.hierarchy.Hierarchies.TopicHierarchy;
import com.flipkart.varadhi.web.hierarchy.ResourceHierarchy;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.flipkart.varadhi.common.Constants.ContextKeys.REQUEST_BODY;
import static com.flipkart.varadhi.common.Constants.MethodNames.GET;
import static com.flipkart.varadhi.common.Constants.MethodNames.LIST;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_TOPIC;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_GET;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_LIST;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_UPDATE;

/**
 * REST handlers for the topic-failover lifecycle. Each endpoint forwards to the controller (which
 * owns orchestration) over the cluster bus via {@link ControllerApi}.
 *
 * <ul>
 *   <li>POST {@code /v1/projects/:project/topics/:topic/failover} — request a failover
 *       (source→target). Returns {@code 202} with the intent op once durably recorded.</li>
 *   <li>GET  {@code /v1/projects/:project/topics/:topic/failover} — in-flight transition
 *       (includes stageHistory); 404 if none active.</li>
 *   <li>POST {@code /v1/projects/:project/topics/:topic/failover/abort} — records abort requested;
 *       honored by the failover executor while the stage is abortable.</li>
 *   <li>GET  {@code /v1/projects/:project/failovers} — active transitions for that project.</li>
 * </ul>
 */
@Slf4j
@ExtensionMethod ({RequestBodyExtension.class, RoutingContextExtension.class})
public class TopicFailoverHandlers implements RouteProvider {

    private static final String API_NAME = "TOPIC_FAILOVER";

    private final ControllerApi controllerClient;
    private final ResourceReadCache<Resource.EntityResource<Project>> projectCache;
    private final VaradhiTopicService topicService;
    private final RegionService regionService;
    private final RegionName deployedRegion;

    public TopicFailoverHandlers(
        ControllerApi controllerClient,
        ResourceReadCache<Resource.EntityResource<Project>> projectCache,
        VaradhiTopicService topicService,
        RegionService regionService,
        RegionName deployedRegion
    ) {
        this.controllerClient = controllerClient;
        this.projectCache = projectCache;
        this.topicService = topicService;
        this.regionService = regionService;
        this.deployedRegion = deployedRegion;
    }

    @Override
    public List<RouteDefinition> get() {
        List<RouteDefinition> routes = new ArrayList<>(
            new SubRoutes(
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
            ).get()
        );
        routes.addAll(
            new SubRoutes(
                "/v1/projects/:project",
                List.of(
                    RouteDefinition.get(LIST, API_NAME, "/failovers")
                                   .nonBlocking()
                                   .authorize(TOPIC_LIST)
                                   .build(this::getProjectHierarchy, this::list)
                )
            ).get()
        );
        return routes;
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

    public Map<ResourceType, ResourceHierarchy> getProjectHierarchy(RoutingContext ctx, boolean hasBody) {
        String projectName = ctx.request().getParam(PATH_PARAM_PROJECT);
        Project project = projectCache.getOrThrow(projectName).getEntity();
        return Map.of(ResourceType.PROJECT, new ProjectHierarchy(project));
    }

    public void create(RoutingContext ctx) {
        TopicFailoverRequest body = ctx.get(REQUEST_BODY);
        RegionName source = body.sourceRegion() != null ? body.sourceRegion() : deployedRegion;
        RegionName target = body.targetRegion();
        VaradhiTopicName topicFqn = getVaradhiTopicName(ctx);
        // Topic and region reads hit the metastore; keep them off the event loop.
        Future<Void> checked = ctx.vertx().executeBlocking(() -> {
            validateFailoverRegions(topicService.get(topicFqn.toFqn()), source, target);
            return (Void)null;
        });
        checked.onComplete(ar -> {
            if (ar.failed()) {
                ctx.fail(ar.cause());
                return;
            }
            TopicFailoverRequest resolved = new TopicFailoverRequest(
                source,
                target,
                body.waitForReplicationLagToClear(),
                body.lagDrainTimeoutMs(),
                body.onLagTimeout()
            );
            controllerClient.createTopicFailover(topicFqn, resolved, ctx.getIdentityOrDefault())
                            .whenComplete((op, error) -> ctx.vertx().runOnContext(v -> {
                                if (error != null) {
                                    ctx.fail(
                                        error instanceof java.util.concurrent.CompletionException ?
                                            error.getCause() :
                                            error
                                    );
                                } else {
                                    ctx.endRequestWithResponse(HttpResponseStatus.ACCEPTED.code(), op);
                                }
                            }));
        });
    }

    public void status(RoutingContext ctx) {
        ctx.handleResponse(controllerClient.getTopicFailover(getVaradhiTopicName(ctx)));
    }

    public void abort(RoutingContext ctx) {
        controllerClient.abortTopicFailover(getVaradhiTopicName(ctx), ctx.getIdentityOrDefault())
                        .whenComplete((transition, error) -> ctx.vertx().runOnContext(v -> {
                            if (error != null) {
                                ctx.fail(
                                    error instanceof java.util.concurrent.CompletionException ? error.getCause() : error
                                );
                            } else {
                                ctx.endRequestWithResponse(HttpResponseStatus.ACCEPTED.code(), transition);
                            }
                        }));
    }

    public void list(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        ctx.handleResponse(
            controllerClient.getActiveFailovers()
                            .thenApply(
                                transitions -> transitions.stream()
                                                          .filter(t -> belongsToProject(t.getTopicFqn(), projectName))
                                                          .toList()
                            )
        );
    }

    private static boolean belongsToProject(String topicFqn, String projectName) {
        return projectName.equals(VaradhiTopicName.parse(topicFqn).getProjectName());
    }

    void validateFailoverRegions(VaradhiTopic topic, RegionName source, RegionName target) {
        if (source == null || target == null) {
            throw new IllegalArgumentException("sourceRegion and targetRegion are required for failover.");
        }
        if (source.equals(target)) {
            throw new IllegalArgumentException("sourceRegion and targetRegion must differ.");
        }
        requireRegisteredRegion(source);
        requireRegisteredRegion(target);
        if (topic.getProduceConfig(source).isEmpty()) {
            throw new IllegalArgumentException(
                "Topic " + topic.getName() + " is not configured for sourceRegion " + source.value() + "."
            );
        }
        if (topic.getProduceConfig(target).isEmpty()) {
            throw new IllegalArgumentException(
                "Topic " + topic.getName() + " is not configured for targetRegion " + target.value() + "."
            );
        }
        if (topic.getSegmentedStorageTopic() == null || topic.getSegmentedStorageTopic().getStorageTopics() == null
            || topic.getSegmentedStorageTopic().getStorageTopics().length == 0) {
            throw new IllegalArgumentException("Topic " + topic.getName() + " has no storage topic.");
        }
        TopicState sourceState = topic.getProduceConfig(source).orElseThrow().getState();
        if (sourceState != TopicState.Producing) {
            throw new IllegalArgumentException(
                "sourceRegion " + source.value() + " is not producing (state=" + sourceState + ")."
            );
        }
    }

    private void requireRegisteredRegion(RegionName region) {
        if (!regionService.regionExists(region.value())) {
            throw new IllegalArgumentException("Region " + region.value() + " is not registered.");
        }
    }

    private VaradhiTopicName getVaradhiTopicName(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        String topicName = ctx.pathParam(PATH_PARAM_TOPIC);
        return VaradhiTopicName.of(projectName, topicName);
    }
}
