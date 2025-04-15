package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.entities.LifecycleStatus;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.web.entities.ResourceActionRequest;
import com.flipkart.varadhi.entities.ResourceDeletionType;
import com.flipkart.varadhi.entities.ResourceHierarchy;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.services.VaradhiTopicService;
import com.flipkart.varadhi.utils.VaradhiTopicFactory;
import com.flipkart.varadhi.web.Extensions.RequestBodyExtension;
import com.flipkart.varadhi.web.Extensions.RoutingContextExtension;
import com.flipkart.varadhi.web.entities.TopicResource;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.flipkart.varadhi.common.Constants.CONTEXT_KEY_BODY;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.common.Constants.PathParams.PATH_PARAM_TOPIC;
import static com.flipkart.varadhi.common.Constants.QueryParams.QUERY_PARAM_DELETION_TYPE;
import static com.flipkart.varadhi.common.Constants.QueryParams.QUERY_PARAM_INCLUDE_INACTIVE;
import static com.flipkart.varadhi.common.Constants.QueryParams.QUERY_PARAM_MESSAGE;
import static com.flipkart.varadhi.entities.Hierarchies.ProjectHierarchy;
import static com.flipkart.varadhi.entities.Hierarchies.TopicHierarchy;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR_REGEX;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_CREATE;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_DELETE;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_GET;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_LIST;
import static com.flipkart.varadhi.entities.auth.ResourceAction.TOPIC_UPDATE;

/**
 * Handler class for managing topics in the Varadhi.
 */
@Slf4j
@ExtensionMethod ({RequestBodyExtension.class, RoutingContextExtension.class})
public class TopicHandlers implements RouteProvider {
    public static final String HANDLER_NAME = "TopicHandlers";

    private final VaradhiTopicFactory varadhiTopicFactory;
    private final VaradhiTopicService varadhiTopicService;
    private final ProjectService projectService;

    /**
     * Constructs a new TopicHandlers instance.
     *
     * @param varadhiTopicFactory the factory for creating VaradhiTopic instances
     * @param varadhiTopicService the service for managing VaradhiTopic instances
     * @param projectService      the service for managing projects
     */
    public TopicHandlers(
        VaradhiTopicFactory varadhiTopicFactory,
        VaradhiTopicService varadhiTopicService,
        ProjectService projectService
    ) {
        this.varadhiTopicFactory = varadhiTopicFactory;
        this.varadhiTopicService = varadhiTopicService;
        this.projectService = projectService;
    }

    /**
     * Returns the list of route definitions for topic management.
     *
     * @return the list of route definitions
     */
    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
            "/v1/projects/:project/topics",
            List.of(
                RouteDefinition.get("GetTopic", HANDLER_NAME, "/:topic")
                               .authorize(TOPIC_GET)
                               .build(this::getHierarchies, this::get),
                RouteDefinition.post("CreateTopic", HANDLER_NAME, "")
                               .hasBody()
                               .bodyParser(this::setRequestBody)
                               .authorize(TOPIC_CREATE)
                               .build(this::getHierarchies, this::create),
                RouteDefinition.delete("DeleteTopic", HANDLER_NAME, "/:topic")
                               .authorize(TOPIC_DELETE)
                               .build(this::getHierarchies, this::delete),
                RouteDefinition.get("ListTopics", HANDLER_NAME, "")
                               .authorize(TOPIC_LIST)
                               .build(this::getHierarchies, this::listTopics),
                RouteDefinition.patch("RestoreTopic", HANDLER_NAME, "/:topic/restore")
                               .authorize(TOPIC_UPDATE)
                               .build(this::getHierarchies, this::restore)
            )
        ).get();
    }

    /**
     * Sets the request body in the routing context.
     *
     * @param ctx the routing context
     */
    public void setRequestBody(RoutingContext ctx) {
        TopicResource topicResource = ctx.body().asValidatedPojo(TopicResource.class);
        ctx.put(CONTEXT_KEY_BODY, topicResource);
    }

    /**
     * Retrieves the resource hierarchies for authorization.
     *
     * @param ctx     the routing context
     * @param hasBody whether the request has a body
     *
     * @return the map of resource types to resource hierarchies
     */
    public Map<ResourceType, ResourceHierarchy> getHierarchies(RoutingContext ctx, boolean hasBody) {
        String projectName = ctx.request().getParam(PATH_PARAM_PROJECT);
        Project project = projectService.getCachedProject(projectName);

        if (hasBody) {
            TopicResource topicResource = ctx.get(CONTEXT_KEY_BODY);
            return Map.of(ResourceType.TOPIC, new TopicHierarchy(project, topicResource.getName()));
        }

        String topicName = ctx.request().getParam(PATH_PARAM_TOPIC);
        if (topicName == null) {
            return Map.of(ResourceType.PROJECT, new ProjectHierarchy(project));
        }

        return Map.of(ResourceType.TOPIC, new TopicHierarchy(project, topicName));
    }

    /**
     * Handles the GET request to retrieve a topic.
     *
     * @param ctx the routing context
     */
    public void get(RoutingContext ctx) {
        VaradhiTopic varadhiTopic = varadhiTopicService.get(getVaradhiTopicName(ctx));
        ctx.endApiWithResponse(TopicResource.from(varadhiTopic));
    }

    /**
     * Handles the POST request to create a new topic.
     *
     * @param ctx the routing context
     */
    public void create(RoutingContext ctx) {
        // TODO: Consider using Vertx ValidationHandlers to validate the request body.
        // TODO: Consider implementing rollback mechanisms for failure scenarios and ≠≠ kind of semantics for all operations.
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        TopicResource topicResource = ctx.get(CONTEXT_KEY_BODY);
        String requestedBy = ctx.getIdentityOrDefault();

        topicResource.setActorCode(getActorCode(requestedBy));

        validateProjectName(projectName, topicResource);

        Project project = projectService.getCachedProject(topicResource.getProject());

        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        varadhiTopicService.create(varadhiTopic, project);
        ctx.endApiWithResponse(TopicResource.from(varadhiTopic));
    }

    /**
     * Handles the DELETE request to delete a topic.
     *
     * @param ctx the routing context
     */
    public void delete(RoutingContext ctx) {
        ResourceDeletionType deletionType = ctx.queryParam(QUERY_PARAM_DELETION_TYPE)
                                               .stream()
                                               .map(ResourceDeletionType::fromValue)
                                               .findFirst()
                                               .orElse(ResourceDeletionType.SOFT_DELETE);
        ResourceActionRequest actionRequest = createResourceActionRequest(ctx);

        varadhiTopicService.delete(getVaradhiTopicName(ctx), deletionType, actionRequest);
        ctx.endApi();
    }

    /**
     * Handles the POST request to restore a topic.
     *
     * @param ctx the routing context
     */
    public void restore(RoutingContext ctx) {
        ResourceActionRequest actionRequest = createResourceActionRequest(ctx);

        varadhiTopicService.restore(getVaradhiTopicName(ctx), actionRequest);
        ctx.endApi();
    }

    /**
     * Handles the GET request to list topics for a project.
     *
     * @param ctx the routing context
     *            - includeInactive: query parameter to include inactive or soft-deleted topics
     */
    public void listTopics(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        boolean includeInactive = ctx.queryParam(QUERY_PARAM_INCLUDE_INACTIVE)
                                     .stream()
                                     .findFirst()
                                     .map(Boolean::parseBoolean)
                                     .orElse(false);

        List<String> topics = varadhiTopicService.getVaradhiTopics(projectName, includeInactive)
                                                 .stream()
                                                 .filter(topic -> topic.startsWith(projectName + NAME_SEPARATOR))
                                                 .map(topic -> topic.split(NAME_SEPARATOR_REGEX)[1])
                                                 .toList();

        ctx.endApiWithResponse(topics);
    }

    /**
     * Retrieves the full topic name from the routing context.
     *
     * @param ctx the routing context
     *
     * @return the full topic name
     */
    private String getVaradhiTopicName(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        String topicName = ctx.pathParam(PATH_PARAM_TOPIC);
        return buildTopicName(projectName, topicName);
    }

    /**
     * Validates that the project name in the URL matches the project name in the request body.
     *
     * @param projectName   the project name from the URL
     * @param topicResource the topic resource from the request body
     *
     * @throws IllegalArgumentException if the project names do not match
     */
    private void validateProjectName(String projectName, TopicResource topicResource) {
        if (!projectName.equals(topicResource.getProject())) {
            throw new IllegalArgumentException("Project name in URL and request body do not match.");
        }
    }

    /**
     * Builds the full topic name from the project name and topic name.
     *
     * @param projectName the project name
     * @param topicName   the topic name
     *
     * @return the full topic name
     */
    private String buildTopicName(String projectName, String topicName) {
        return String.join(NAME_SEPARATOR, projectName, topicName);
    }

    /**
     * Creates a resource action request from the routing context.
     *
     * @param ctx the routing context
     *
     * @return the resource action request
     */
    private ResourceActionRequest createResourceActionRequest(RoutingContext ctx) {
        String requestedBy = ctx.getIdentityOrDefault();
        LifecycleStatus.ActorCode actorCode = isVaradhiAdmin(requestedBy) ?
            LifecycleStatus.ActorCode.ADMIN_ACTION :
            LifecycleStatus.ActorCode.USER_ACTION;
        String message = ctx.queryParam(QUERY_PARAM_MESSAGE).stream().findFirst().orElse("");
        return new ResourceActionRequest(actorCode, message);
    }

    /**
     * Checks if the identity is a Varadhi admin.
     * TODO: Replace with a call to isVaradhiAdmin(requestedBy) when authorization is implemented.
     *
     * @param identity the identity to check
     *
     * @return true if the identity is a Varadhi admin, false otherwise
     */
    private boolean isVaradhiAdmin(String identity) {
        return Objects.equals(identity, "varadhi-admin");
    }

    /**
     * Determines the actor code based on the identity of the requester.
     *
     * @param requestedBy the identity of the requester
     *
     * @return the actor code, either ADMIN_ACTION if the requester is a Varadhi admin, or USER_ACTION otherwise
     */
    private LifecycleStatus.ActorCode getActorCode(String requestedBy) {
        return isVaradhiAdmin(requestedBy) ?
            LifecycleStatus.ActorCode.ADMIN_ACTION :
            LifecycleStatus.ActorCode.USER_ACTION;
    }
}
