package com.flipkart.varadhi.auth;

import com.flipkart.varadhi.authz.AuthorizationProvider;
import com.flipkart.varadhi.config.AuthorizationOptions;
import com.flipkart.varadhi.config.DefaultAuthorizationConfiguration;
import com.flipkart.varadhi.entities.ResourceAction;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.entities.Role;
import com.flipkart.varadhi.entities.UserContext;
import com.flipkart.varadhi.exceptions.IllegalArgumentException;
import com.flipkart.varadhi.utils.YamlLoader;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.codec.BodyCodec;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;


@Slf4j
public class DefaultAuthorizationProvider implements AuthorizationProvider {
    private static final Role EMPTY_ROLE = new Role("", Set.of());
    private DefaultAuthorizationConfiguration configuration;
    private WebClient webClient;
    private volatile boolean initialised = false;

    @Override
    public synchronized Future<Boolean> init(AuthorizationOptions authorizationOptions) {
        if (!this.initialised) {
            this.configuration =
                    YamlLoader.loadConfig(
                            authorizationOptions.getConfigFile(), DefaultAuthorizationConfiguration.class);
            this.webClient = WebClient.create(Vertx.vertx()); //CachingWebClient.create(WebClient.create(Vertx.vertx()));
            this.initialised = true;
        }
        return Future.succeededFuture(true);
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
        List<Pair<ResourceType, String>> leafToRootResourceIds = generatePolicyPathsForResource(action, resource);
        if (leafToRootResourceIds.isEmpty()) {
            return Future.succeededFuture(false);
        }

        List<Future<Boolean>> futures =
                leafToRootResourceIds.stream().filter(pair -> StringUtils.isNotBlank(pair.getValue()))
                        .map(pair -> isAuthorizedInternal(userContext.getSubject(), action, pair.getValue()))
                        .toList();
        // succeeded future denotes successful authorization
        return Future.any(futures).transform(result -> Future.succeededFuture(result.succeeded()));
    }

    /**
     * Parse the resource path based on the action and resolve the final resourceIDs for each resourceType.
     *
     * @param resourcePath uri of the resource
     *
     * @return List of pairs having resource type to its id. List is used so that we can impose ordering from leaf to root nodes.
     */
    private List<Pair<ResourceType, String>> generatePolicyPathsForResource(
            ResourceAction action, String resourcePath
    ) {
        String[] segments = resourcePath.split("/");

        // build the list in reverse order specified: ROOT -> ORG -> TEAM -> PROJECT -> TOPIC|SUBSCRIPTION|QUEUE
        List<Pair<ResourceType, String>> resourceIdTuples = new ArrayList<>();
        addGetPolicyPathForLeafNode(resourceIdTuples, action, segments);
        addGetPolicyPathForProjectNode(resourceIdTuples, segments);
        addGetPolicyPathForTeamNode(resourceIdTuples, segments);
        addGetPolicyPathForOrgNode(resourceIdTuples, segments);

        return resourceIdTuples;
    }

    /**
     * Authorize subject against a single resource.
     * Succeeds the future if user is authorized, otherwise fails the future.
     *
     * @param subject            user identifier to be authorized
     * @param action             action requested by the subject which needs authorization
     * @param resourcePolicyPath get IAM Policy path under whose scope the check will be performed
     *
     * @return {@code Future<Boolean>} return a future of true or failed future if not authorized
     */
    private Future<Boolean> isAuthorizedInternal(String subject, ResourceAction action, String resourcePolicyPath) {
        log.debug(
                "Checking authorization for subject [{}] and action [{}] on resource [{}]", subject, action,
                resourcePolicyPath
        );
        return getRolesForSubject(subject, resourcePolicyPath)
                .compose(roles -> (checkAllRoles(subject, roles, action)) ?
                        Future.succeededFuture(true)
                        :
                        Future.failedFuture("Not authorized"));
    }


    protected Future<Set<String>> getRolesForSubject(String subject, String resourcePolicyPath) {
        // use web client to make get call
        return webClient.get(
                        configuration.getAuthZServerPort(), configuration.getAuthZServerHost(),
                        String.join("/", "/v1", resourcePolicyPath, "policy")
                )
                .as(BodyCodec.json(RoleBindingNode.class))
                .send()
                .compose(response -> {
                    RoleBindingNode node = response.body();
                    if (node == null || node.getSubjectToRolesMapping() == null) {
                        return Future.failedFuture("No roles on resource for subject" + subject);
                    }
                    log.info(
                            "Fetched roles for subject [{}] and resource [{}] status: {}", subject, resourcePolicyPath,
                            response.statusCode()
                    );
                    return Future.succeededFuture(node.getSubjectToRolesMapping().getOrDefault(subject, Set.of()));
                })
                .onFailure(err -> log.error("Error while fetching roles for subject [{}] and resource [{}]",
                        subject, resourcePolicyPath, err
                ));
    }

    private boolean doesActionBelongToRole(String subject, String roleId, ResourceAction action) {
        log.debug("Evaluating action [{}] for subject [{}] against role [{}]", action, subject, roleId);
        boolean matching =
                configuration.getRoleDefinitions().getOrDefault(roleId, EMPTY_ROLE).getPermissions().contains(action);
        if (matching) {
            log.debug("Successfully matched action [{}] for subject [{}] against role [{}]", action, subject, roleId);
        }
        return matching;
    }

    private boolean checkAllRoles(String subject, Set<String> roles, ResourceAction action) {
        return roles.stream().anyMatch(role -> doesActionBelongToRole(subject, role, action));
    }

    private void addGetPolicyPathForOrgNode(List<Pair<ResourceType, String>> resourceIdTuples, String[] segments) {
        if (segments.length > 0) {
            resourceIdTuples.add(Pair.of(ResourceType.ORG, String.format("/orgs/%s", segments[0])));
        }
    }

    private void addGetPolicyPathForTeamNode(List<Pair<ResourceType, String>> resourceIdTuples, String[] segments) {
        if (segments.length > 1) {
            // /orgs/:org/teams/:team
            resourceIdTuples.add(
                    Pair.of(ResourceType.TEAM, String.format("/orgs/%s/teams/%s", segments[0], segments[1])));
        }
    }

    private void addGetPolicyPathForProjectNode(List<Pair<ResourceType, String>> resourceIdTuples, String[] segments) {
        if (segments.length > 2) {
            resourceIdTuples.add(Pair.of(ResourceType.PROJECT, String.format("/projects/%s", segments[2])));
        }
    }

    private void addGetPolicyPathForLeafNode(
            List<Pair<ResourceType, String>> resourceIdTuples, ResourceAction action, String[] segments
    ) {
        if (segments.length > 3) {
            // /projects/:project/[topics|subs]/:[topic|sub]
            switch (action.getResourceType()) {
                case TOPIC -> resourceIdTuples.add(
                        Pair.of(ResourceType.TOPIC, String.format("/projects/%s/topics/%s", segments[2], segments[3])));
                case SUBSCRIPTION -> resourceIdTuples.add(Pair.of(ResourceType.SUBSCRIPTION,
                        String.format("/projects/%s/subscriptions/%s", segments[2], segments[3])
                ));
                default -> throw new IllegalArgumentException(
                        "Invalid resource type under project : " + action.getResourceType());
            }
        }
    }
}
