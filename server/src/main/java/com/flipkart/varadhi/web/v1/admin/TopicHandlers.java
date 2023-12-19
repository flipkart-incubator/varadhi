package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.core.VaradhiTopicFactory;
import com.flipkart.varadhi.core.VaradhiTopicService;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.TopicResource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.exceptions.ArgumentException;
import com.flipkart.varadhi.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.web.Extensions.RequestBodyExtension;
import com.flipkart.varadhi.web.Extensions.RoutingContextExtension;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_PROJECT;
import static com.flipkart.varadhi.Constants.PathParams.REQUEST_PATH_PARAM_TOPIC;
import static com.flipkart.varadhi.entities.ResourceAction.*;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.authenticated;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.hasBody;

@Slf4j
@ExtensionMethod({RequestBodyExtension.class, RoutingContextExtension.class})
public class TopicHandlers implements RouteProvider {
    private final VaradhiTopicFactory varadhiTopicFactory;
    private final VaradhiTopicService varadhiTopicService;
    private final MetaStore metaStore;

    public TopicHandlers(
            VaradhiTopicFactory varadhiTopicFactory,
            VaradhiTopicService varadhiTopicService,
            MetaStore metaStore
    ) {
        this.varadhiTopicFactory = varadhiTopicFactory;
        this.varadhiTopicService = varadhiTopicService;
        this.metaStore = metaStore;
    }

    @Override
    public List<RouteDefinition> get() {
        return new SubRoutes(
                "/v1/projects/:project/topics",
                List.of(
                        new RouteDefinition(
                                HttpMethod.GET,
                                "/:topic",
                                Set.of(),
                                new LinkedHashSet<>(),
                                this::get,
                                true,
                                Optional.of(PermissionAuthorization.of(TOPIC_GET, "{project}/{topic}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.POST,
                                "",
                                Set.of(authenticated, hasBody),
                                new LinkedHashSet<>(),
                                this::create,
                                true,
                                Optional.of(PermissionAuthorization.of(TOPIC_CREATE, "{project}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.DELETE,
                                "/:topic",
                                Set.of(),
                                new LinkedHashSet<>(),
                                this::delete,
                                true,
                                Optional.of(PermissionAuthorization.of(TOPIC_DELETE, "{project}/{topic}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.GET,
                                "",
                                Set.of(),
                                new LinkedHashSet<>(),
                                this::getTopics,
                                true,
                                Optional.of(PermissionAuthorization.of(TOPIC_GET, "{project}")) //TODO: Do we need a new permission for this?
                        )
                )
        ).get();
    }

    public void get(RoutingContext ctx) {
        String projectName = ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
        String topicResourceName = ctx.pathParam(REQUEST_PATH_PARAM_TOPIC);
        VaradhiTopic varadhiTopic = metaStore.getVaradhiTopic(topicResourceName, projectName);
        TopicResource topicResource = varadhiTopic.getTopicResource(projectName);
        ctx.endApiWithResponse(topicResource);
    }

    public void create(RoutingContext ctx) {
        //TODO:: Enable authn/authz for this flow.
        //TODO:: Consider using Vertx ValidationHandlers to validate the request body.
        //TODO:: Consider reverting on failure and ≠≠ kind of semantics for all operations.

        String projectName = ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
        TopicResource topicResource = ctx.body().asValidatedPojo(TopicResource.class);
        if (!projectName.equals(topicResource.getProject())) {
            throw new ArgumentException("Specified Project name is different from Project name in url");
        }

        Project project = metaStore.getProject(topicResource.getProject());
        boolean found = metaStore.checkVaradhiTopicExists(topicResource.getName(), topicResource.getProject());
        if (found) {
            log.error("Specified Topic({}:{}) already exists.", topicResource.getProject(), topicResource.getName());
            throw new DuplicateResourceException(
                    String.format("Specified Topic(%s:%s) already exists.", topicResource.getProject(),
                            topicResource.getName()
                    ));
        }
        VaradhiTopic vt = varadhiTopicFactory.get(project, topicResource);
        varadhiTopicService.create(vt, project);
        ctx.endApiWithResponse(topicResource);
    }


    public void delete(RoutingContext ctx) {
        String projectName = ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
        String topicResourceName = ctx.pathParam(REQUEST_PATH_PARAM_TOPIC);
        VaradhiTopic varadhiTopic = metaStore.getVaradhiTopic(topicResourceName, projectName);
        varadhiTopicService.delete(varadhiTopic);
        ctx.endApi();
    }

    public void getTopics(RoutingContext ctx) {
        String projectName = ctx.pathParam(REQUEST_PATH_PARAM_PROJECT);
        List<String> topicNames = metaStore.getTopicResourceNames(projectName);
        ctx.endApiWithResponse(topicNames);
    }
}
