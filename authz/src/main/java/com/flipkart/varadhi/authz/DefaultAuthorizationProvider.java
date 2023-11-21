package com.flipkart.varadhi.authz;

import com.flipkart.varadhi.config.AuthorizationOptions;
import com.flipkart.varadhi.config.DefaultAuthorizationConfiguration;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import com.flipkart.varadhi.utils.YamlLoader;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.CachingWebClient;
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
    public synchronized Future<Boolean> init(Vertx vertx, AuthorizationOptions authorizationOptions) {
        if (!this.initialised) {
            this.configuration =
                    YamlLoader.loadConfig(
                            authorizationOptions.getConfigFile(), DefaultAuthorizationConfiguration.class);
            this.webClient = CachingWebClient.create(WebClient.create(vertx));
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
            throw new InvalidConfigException("Default Authorization Provider is not initialised.");
        }
        List<Pair<ResourceType, String>> leafToRootResourceIds = resolveOrderedFromLeaf(action, resource);
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
    private List<Pair<ResourceType, String>> resolveOrderedFromLeaf(ResourceAction action, String resourcePath) {
        String[] segments = resourcePath.split("/");

        // build the list in reverse order specified: ROOT -> ORG -> TEAM -> PROJECT -> TOPIC|SUBSCRIPTION|QUEUE
        List<Pair<ResourceType, String>> resourceIdTuples = new ArrayList<>();
        pushLeafNode(resourceIdTuples, action, segments);
        pushProjectNode(resourceIdTuples, segments);
        pushTeamNode(resourceIdTuples, segments);
        pushOrgNode(resourceIdTuples, segments);

        return resourceIdTuples;
    }

    /**
     * Authorize subject against a single resourceId.
     * Succeeds the future if user is authorized, otherwise fails the future.
     *
     * @param subject    user identifier to be authorized
     * @param action     action requested by the subject which needs authorization
     * @param resourceId resource id under whose scope the check will be performed
     *
     * @return {@code Future<Boolean>} a future result expressing True/False decision
     */
    private Future<Boolean> isAuthorizedInternal(String subject, ResourceAction action, String resourceId) {
        log.debug(
                "Checking authorization for subject [{}] and action [{}] on resource [{}]", subject, action,
                resourceId
        );
        return getRolesForSubject(subject, resourceId)
                .compose(roles -> (checkAllRoles(subject, roles, action)) ?
                        Future.succeededFuture(true)
                        :
                        Future.failedFuture("Not authorized"));
    }


    protected Future<Set<String>> getRolesForSubject(String subject, String resourceId) {
        // use web client to make get call
        return webClient.get(8088, "localhost", "/v1/authz/rbs/" + resourceId)
                .as(BodyCodec.json(RoleBindingNode.class))
                .send()
                .compose(response -> {
                    RoleBindingNode node = response.body();
                    return Future.succeededFuture(node.getSubjectToRolesMapping().getOrDefault(subject, Set.of()));
                })
                .onFailure(err -> log.error("Error while fetching roles for subject [{}] and resource [{}]",
                        subject, resourceId, err
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

    private void pushOrgNode(List<Pair<ResourceType, String>> resourceIdTuples, String[] segments) {
        if (segments.length > 0) {
            resourceIdTuples.add(Pair.of(ResourceType.ORG, segments[0]));
        }
    }

    private void pushTeamNode(List<Pair<ResourceType, String>> resourceIdTuples, String[] segments) {
        if (segments.length > 1) {
            resourceIdTuples.add(Pair.of(ResourceType.TEAM, segments[0] + ":" + segments[1])); //{org_id}:{team_id}
        }
    }

    private void pushProjectNode(List<Pair<ResourceType, String>> resourceIdTuples, String[] segments) {
        if (segments.length > 2) {
            resourceIdTuples.add(Pair.of(ResourceType.PROJECT, segments[2]));
        }
    }

    private void pushLeafNode(
            List<Pair<ResourceType, String>> resourceIdTuples, ResourceAction action, String[] segments
    ) {
        if (segments.length > 3) {
            resourceIdTuples.add(Pair.of(
                    action.getResourceType(),
                    segments[2] + ":" + segments[3]
            )); //{project_id}:{[topic|sub|queue]_id}
        }
    }
}
