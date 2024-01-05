package com.flipkart.varadhi.auth;

import com.flipkart.varadhi.authz.AuthorizationOptions;
import com.flipkart.varadhi.authz.AuthorizationProvider;
import com.flipkart.varadhi.config.DefaultAuthorizationConfiguration;
import com.flipkart.varadhi.entities.auth.*;
import com.flipkart.varadhi.services.AuthZService;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreOptions;
import com.flipkart.varadhi.spi.db.MetaStoreProvider;
import com.flipkart.varadhi.spi.db.RoleBindingMetaStore;
import com.flipkart.varadhi.utils.YamlLoader;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.flipkart.varadhi.utils.LoaderUtils.loadClass;


@Slf4j
public class DefaultAuthorizationProvider implements AuthorizationProvider {
    private static final Role EMPTY_ROLE = new Role("", Set.of());
    private DefaultAuthorizationConfiguration configuration;
    private AuthZService authZService;
    private volatile boolean initialised = false;

    @Override
    public Future<Boolean> init(AuthorizationOptions authorizationOptions) {
        if (!this.initialised) {
            this.configuration =
                    YamlLoader.loadConfig(
                            authorizationOptions.getConfigFile(), DefaultAuthorizationConfiguration.class);
            getAuthZService();
            this.initialised = true;
        }
        return Future.succeededFuture(true);
    }

    protected AuthZService getAuthZService() {
        if (this.initialised) {
            return this.authZService;
        }
        this.authZService = initAuthZService(this.configuration.getMetaStoreOptions());
        return this.authZService;
    }

    private AuthZService initAuthZService(MetaStoreOptions options) {
        MetaStoreProvider provider = loadClass(options.getProviderClassName());
        provider.init(options);
        MetaStore store = provider.getMetaStore();

        if (store instanceof RoleBindingMetaStore) {
            return new AuthZService(store, (RoleBindingMetaStore) store);
        }

        throw new IllegalStateException("Provider must implement RoleBindingMetaStore");
    }

    /**
     * Checks if the user {@code UserContext} is authorized to perform action {@code ResourceAction} on resource path.
     *
     * @param userContext Information on the user, contains the user identifier
     * @param action      Action being performed by the user which is to be authorized
     * @param resource    Full schemaless URI of the resource on which the action is to be authorized.
     *                    Must be of format: {org_id}/{team_id}/{project_id}/{topic|queue|subscription}
     *
     * @return {@code Future<Boolean>} a future result expressing True/False decision
     */
    @Override
    public Future<Boolean> isAuthorized(UserContext userContext, ResourceAction action, String resource) {
        if (!initialised) {
            throw new IllegalStateException("Default Authorization Provider is not initialised.");
        }
        List<Pair<ResourceType, ResourceContext>> leafToRootResourceIds =
                generateResourceContextHierarchy(action, resource);
        if (leafToRootResourceIds.isEmpty()) {
            return Future.succeededFuture(false);
        }

        boolean result =
                leafToRootResourceIds.stream()
                        .anyMatch(pair -> isAuthorizedInternal(userContext.getSubject(), action, pair.getValue()));
        return Future.succeededFuture(result);
    }

    /**
     * Parse the resource path based on the action and resolve the final resourceIDs for each resourceType.
     *
     * @param resourcePath uri of the resource
     *
     * @return List of pairs having resource type to its context. List is used so that we can impose ordering from leaf to root nodes.
     */
    private List<Pair<ResourceType, ResourceContext>> generateResourceContextHierarchy(
            ResourceAction action, String resourcePath
    ) {
        if (StringUtils.isBlank(resourcePath)) {
            return List.of();
        }

        String[] segments = resourcePath.split("/");

        // build the list in reverse order specified: ROOT -> ORG -> TEAM -> PROJECT -> TOPIC|SUBSCRIPTION|QUEUE
        List<Pair<ResourceType, ResourceContext>> resourceContextTuples = new ArrayList<>();
        addResourceContextForLeafNode(resourceContextTuples, action, segments);
        addResourceContextForProjectNode(resourceContextTuples, segments);
        addResourceContextForTeamNode(resourceContextTuples, segments);
        addResourceContextForOrgNode(resourceContextTuples, segments);

        return resourceContextTuples;
    }

    /**
     * Authorize subject against a single resource.
     * Succeeds the future if user is authorized, otherwise fails the future.
     *
     * @param subject         user identifier to be authorized
     * @param action          action requested by the subject which needs authorization
     * @param resourceContext record holding the resource type, id and policy path
     *
     * @return {@code boolean} return whether the user is authorized or not
     */
    private boolean isAuthorizedInternal(
            String subject, ResourceAction action, ResourceContext resourceContext
    ) {
        return getRolesForSubject(subject, resourceContext)
                .stream().anyMatch(role -> doesActionBelongToRole(subject, role, action));
    }

    private Set<String> getRolesForSubject(String subject, ResourceContext resourceContext) {
        RoleBindingNode node =
                getAuthZService().getIAMPolicy(resourceContext.resourceType(), resourceContext.resourceId());
        if (node == null) {
            log.debug("No roles on resource for subject {}", subject);
            return Set.of();
        }
        return node.getRolesAssignment().getOrDefault(subject, Set.of());
    }

    private boolean doesActionBelongToRole(String subject, String roleId, ResourceAction action) {
        boolean matching =
                configuration.getRoleDefinitions().getOrDefault(roleId, EMPTY_ROLE).getPermissions().contains(action);
        if (matching) {
            log.debug("Successfully matched action [{}] for subject [{}] against role [{}]", action, subject, roleId);
        }
        return matching;
    }

    private void addResourceContextForOrgNode(
            List<Pair<ResourceType, ResourceContext>> resourceIdTuples, String[] segments
    ) {
        resourceIdTuples.add(Pair.of(
                ResourceType.ORG,
                new ResourceContext(ResourceType.ORG, segments[0], "/orgs/%s".formatted(segments[0]))
        ));
    }

    private void addResourceContextForTeamNode(
            List<Pair<ResourceType, ResourceContext>> resourceIdTuples, String[] segments
    ) {
        if (segments.length > 1) {
            // /orgs/:org/teams/:team
            resourceIdTuples.add(
                    Pair.of(
                            ResourceType.TEAM,
                            new ResourceContext(
                                    ResourceType.TEAM, "%s:%s".formatted(segments[0], segments[1]),
                                    "/orgs/%s/teams/%s".formatted(segments[0], segments[1])
                            )
                    ));
        }
    }

    private void addResourceContextForProjectNode(
            List<Pair<ResourceType, ResourceContext>> resourceIdTuples, String[] segments
    ) {
        if (segments.length > 2) {
            resourceIdTuples.add(Pair.of(
                    ResourceType.PROJECT,
                    new ResourceContext(ResourceType.PROJECT, segments[2], "/projects/%s".formatted(segments[2]))
            ));
        }
    }

    private void addResourceContextForLeafNode(
            List<Pair<ResourceType, ResourceContext>> resourceIdTuples, ResourceAction action, String[] segments
    ) {
        if (segments.length > 3) {
            // /projects/:project/[topics|subs]/:[topic|sub]
            switch (action.getResourceType()) {
                case TOPIC -> resourceIdTuples.add(Pair.of(
                        ResourceType.TOPIC,
                        new ResourceContext(
                                ResourceType.TOPIC, "%s:%s".formatted(segments[2], segments[3]),
                                "/projects/%s/topics/%s".formatted(segments[2], segments[3])
                        )
                ));
                case SUBSCRIPTION -> resourceIdTuples.add(Pair.of(
                        ResourceType.SUBSCRIPTION,
                        new ResourceContext(
                                ResourceType.SUBSCRIPTION, "%s:%s".formatted(segments[2], segments[3]),
                                "/projects/%s/subscriptions/%s".formatted(segments[2], segments[3])
                        )
                ));
                default -> throw new IllegalArgumentException(
                        "Invalid resource type under project : " + action.getResourceType());
            }
        }
    }

    private record ResourceContext(ResourceType resourceType, String resourceId, String policyPath) {
    }
}
