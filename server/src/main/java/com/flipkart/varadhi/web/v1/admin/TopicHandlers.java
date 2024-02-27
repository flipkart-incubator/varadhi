package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.core.VaradhiTopicFactory;
import com.flipkart.varadhi.core.VaradhiTopicService;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.services.ProjectService;
import com.flipkart.varadhi.web.Extensions.RequestBodyExtension;
import com.flipkart.varadhi.web.Extensions.RoutingContextExtension;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.varadhi.Constants.CONTEXT_KEY_BODY;
import static com.flipkart.varadhi.Constants.PathParams.*;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR_REGEX;
import static com.flipkart.varadhi.entities.auth.ResourceAction.*;

@Slf4j
@ExtensionMethod({RequestBodyExtension.class, RoutingContextExtension.class})
public class TopicHandlers implements RouteProvider {
    private final VaradhiTopicFactory varadhiTopicFactory;
    private final VaradhiTopicService varadhiTopicService;
    private final ProjectService projectService;

    public TopicHandlers(
            VaradhiTopicFactory varadhiTopicFactory,
            VaradhiTopicService varadhiTopicService,
            ProjectService projectService
    ) {
        this.varadhiTopicFactory = varadhiTopicFactory;
        this.varadhiTopicService = varadhiTopicService;
        this.projectService = projectService;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
                "/v1/projects/:project/topics",
                List.of(
                        RouteDefinition.get("GetTopic", "/:topic")
                                .authorize(TOPIC_GET, "{project}/{topic}")
                                .build(this::getHierarchy, this::get),
                        RouteDefinition.post("CreateTopic", "")
                                .hasBody()
                                .bodyParser(this::setTopic)
                                .authorize(TOPIC_CREATE, "{project}")
                                .build(this::getHierarchy, this::create),
                        RouteDefinition.delete("DeleteTopic", "/:topic")
                                .authorize(TOPIC_DELETE, "{project}/{topic}")
                                .build(this::getHierarchy, this::delete),
                        RouteDefinition.get("ListTopics", "")
                                .authorize(TOPIC_GET, "{project}")
                                .build(this::getHierarchy, this::listTopics)
                )
        ).get();
    }

    public void setTopic(RoutingContext ctx) {
        TopicResource topic = ctx.body().asValidatedPojo(TopicResource.class);
        ctx.put(CONTEXT_KEY_BODY, topic);
    }

    public ResourceHierarchy getHierarchy(RoutingContext ctx, boolean hasBody) {
        String projectName = ctx.request().getParam(PATH_PARAM_PROJECT);
        Project project = projectService.getCachedProject(projectName);
        if (hasBody) {
            TopicResource topicResource = ctx.get(CONTEXT_KEY_BODY);
            return new Hierarchies.TopicHierarchy(
                    project.getOrg(), project.getTeam(), project.getName(), topicResource.getName());
        }
        String topicName = ctx.request().getParam(PATH_PARAM_TOPIC);
        if (null == topicName) {
            return new Hierarchies.ProjectHierarchy(project.getOrg(), project.getTeam(), project.getName());
        }
        return new Hierarchies.TopicHierarchy(project.getOrg(), project.getTeam(), project.getName(), topicName);
    }

    public void get(RoutingContext ctx) {
        String varadhiTopicName = getVaradhiTopicName(ctx);
        VaradhiTopic varadhiTopic = varadhiTopicService.get(varadhiTopicName);
        TopicResource topicResource = TopicResource.of(varadhiTopic);
        ctx.endApiWithResponse(topicResource);
    }

    public void create(RoutingContext ctx) {
        //TODO:: Enable authn/authz for this flow.
        //TODO:: Consider using Vertx ValidationHandlers to validate the request body.
        //TODO:: Consider reverting on failure and ≠≠ kind of semantics for all operations.

        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        TopicResource topicResource = ctx.get(CONTEXT_KEY_BODY);
        if (!projectName.equals(topicResource.getProject())) {
            throw new IllegalArgumentException("Specified Project name is different from Project name in url");
        }

        Project project = projectService.getCachedProject(topicResource.getProject());
        String varadhiTopicName = String.join(NAME_SEPARATOR, projectName, topicResource.getName());
        boolean found = varadhiTopicService.exists(varadhiTopicName);
        if (found) {
            throw new DuplicateResourceException(
                    String.format("Specified Topic(%s) already exists.", varadhiTopicName));
        }
        VaradhiTopic vt = varadhiTopicFactory.get(project, topicResource);
        varadhiTopicService.create(vt, project);
        ctx.endApiWithResponse(topicResource);
    }

    public void delete(RoutingContext ctx) {
        String varadhiTopicName = getVaradhiTopicName(ctx);
        varadhiTopicService.delete(varadhiTopicName);
        ctx.endApi();
    }

    public void listTopics(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        List<String> varadhiTopics = varadhiTopicService.getVaradhiTopics(projectName);

        String projectPrefixOfVaradhiTopic = projectName + NAME_SEPARATOR;
        List<String> topicResourceNames = new ArrayList<>();
        varadhiTopics.forEach(varadhiTopic -> {
                    if (varadhiTopic.startsWith(projectPrefixOfVaradhiTopic)) {
                        String[] splits = varadhiTopic.split(NAME_SEPARATOR_REGEX);
                        topicResourceNames.add(splits[1]);
                    }
                }
        );
        ctx.endApiWithResponse(topicResourceNames);
    }

    private String getVaradhiTopicName(RoutingContext ctx) {
        String projectName = ctx.pathParam(PATH_PARAM_PROJECT);
        String topicResourceName = ctx.pathParam(PATH_PARAM_TOPIC);
        return String.join(NAME_SEPARATOR, projectName, topicResourceName);
    }
}
