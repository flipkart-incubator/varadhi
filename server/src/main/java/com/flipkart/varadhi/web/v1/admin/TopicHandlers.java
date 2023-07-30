package com.flipkart.varadhi.web.v1.admin;

import com.flipkart.varadhi.auth.PermissionAuthorization;
import com.flipkart.varadhi.db.MetaStore;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.TopicResource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.VaradhiTopicFactory;
import com.flipkart.varadhi.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.services.VaradhiTopicService;
import com.flipkart.varadhi.web.Extensions.RequestBodyExtension;
import com.flipkart.varadhi.web.Extensions.RoutingContextExtension;
import com.flipkart.varadhi.web.routes.RouteDefinition;
import com.flipkart.varadhi.web.routes.RouteProvider;
import com.flipkart.varadhi.web.routes.SubRoutes;
import com.google.common.collect.Sets;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.RoutingContext;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.flipkart.varadhi.auth.ResourceAction.*;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.authenticated;
import static com.flipkart.varadhi.web.routes.RouteBehaviour.hasBody;

@Slf4j
@ExtensionMethod({RequestBodyExtension.class, RoutingContextExtension.class})
public class TopicHandlers implements RouteProvider {
    private static final String DEFAULT_TENANT = "public";
    private static final String DEFAULT_TEAM = "public";
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
                "/v1/tenants/:tenant/topics",
                List.of(
                        new RouteDefinition(
                                HttpMethod.GET,
                                "/:topic",
                                Set.of(),
                                Sets.newLinkedHashSet(),
                                this::get,
                                true,
                                Optional.of(PermissionAuthorization.of(TOPIC_GET, "{tenant}/{topic}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.POST,
                                "",
                                Set.of(authenticated, hasBody),
                                Sets.newLinkedHashSet(),
                                this::create,
                                true,
                                Optional.of(PermissionAuthorization.of(TOPIC_CREATE, "{tenant}"))
                        ),
                        new RouteDefinition(
                                HttpMethod.DELETE,
                                "/:topic",
                                Set.of(),
                                Sets.newLinkedHashSet(),
                                this::delete,
                                true,
                                Optional.of(PermissionAuthorization.of(TOPIC_DELETE, "{tenant}/{topic}"))
                        )
                )
        ).get();
    }

    public void get(RoutingContext ctx) {
        ctx.todo();
    }

    public void create(RoutingContext ctx) {
        //TODO:: Enable authn/authz for this flow.
        //TODO:: Consider using Vertx ValidationHandlers to validate the request body.
        //TODO:: Consider reverting on failure and ≠≠ kind of semantics for all operations.

        TopicResource topicResource = ctx.body().asPojo(TopicResource.class);
        //TODO:: fetch project from metastore when implemented.
        Project project = new Project(topicResource.getName(), DEFAULT_TEAM, DEFAULT_TENANT);

        boolean found = metaStore.checkTopicResourceExists(topicResource.getProject(), topicResource.getName());
        if (found) {
            log.error("Specified Topic({}:{}) already exists.", topicResource.getProject(), topicResource.getName());
            throw new DuplicateResourceException(
                    String.format("Specified Topic(%s:%s) already exists.", topicResource.getProject(),
                            topicResource.getName()
                    ));
        }
        TopicResource createdResource = metaStore.createTopicResource(topicResource);
        VaradhiTopic vt = varadhiTopicFactory.get(project, topicResource);
        varadhiTopicService.create(vt);
        ctx.setApiResponse(createdResource);
    }


    public void delete(RoutingContext ctx) {
        ctx.todo();
    }
}
