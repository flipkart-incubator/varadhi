package com.flipkart.varadhi.auth;

import com.flipkart.varadhi.entities.UserContext;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DefaultAuthorizationProvider implements AuthorizationProvider {
    private DefaultAuthorizationConfiguration configuration;

    @Override
    public Future<Boolean> init(JsonObject configuration) {
        this.configuration = configuration.mapTo(DefaultAuthorizationConfiguration.class);
        return Future.succeededFuture(true);
    }

    /**
     * Checks if the user {@code UserContext} is authorized to perform action {@code ResourceAction} on resource path.
     *
     * @param userContext Information on the user, contains the user identifier
     * @param action      Action being performed by the user which is to be authorized
     * @param resource    URI of the resource on which the action is to be authorized.
     *                    Must be of format: {org_id} or {org_id}/{team_id} or {project_id}
     *                    or {project_id}/{topic|queue|subscription}
     *
     * @return {@code Future<Boolean>} a future result expressing True/False decision
     */
    @Override
    public Future<Boolean> isAuthorized(UserContext userContext, ResourceAction action, String resource) {
        // parse the resource path based on the action and return the final resourceIDs for each resourceType
        Map<ResourceType, String> resourceTypeToResourceId = getResourceIdsFromResourceURI(action, resource);
        return Future.succeededFuture(resourceTypeToResourceId.entrySet().stream()
                .anyMatch(entry -> isAuthorizedInternal(userContext.getSubject(), action, entry.getValue())));
    }

    /**
     * Takes the {@code resource} as a URI and breaks it down into individual pairs of {@code ResourceType} and their resourceIds
     *
     * @param action   ResourceAction to validate against (mostly required for resolving leaf nodes in the hierarchy)
     * @param resource Full path to the resource on which the action is being performed (URI without any scheme)
     *
     * @return Mapping of each ResourceType to its respective resourceID in context of the current resource path
     */
    private Map<ResourceType, String> getResourceIdsFromResourceURI(ResourceAction action, String resource) {
        // TODO(aayush): need better way to do this?
        Map<ResourceType, String> resultMap = new HashMap<>();
        String[] segments = resource.split("/");
        return resolve(ResourceType.ORG, action, List.of(segments), resultMap);
    }


    private Map<ResourceType, String> resolve(
            ResourceType current, ResourceAction action, List<String> segments, Map<ResourceType, String> result
    ) {
        if (segments.isEmpty()) {
            return result;
        }
        return switch (current) {
            case ORG -> {
                result.put(ResourceType.ORG, segments.remove(0));
                yield resolve(ResourceType.TEAM, action, segments, result);
            }
            case TEAM -> {
                result.put(ResourceType.TEAM, result.get(ResourceType.ORG) + ":" + segments.remove(0));
                yield resolve(ResourceType.PROJECT, action, segments, result);
            }
            case PROJECT -> {
                result.put(ResourceType.PROJECT, segments.remove(0));
                yield resolve(action.getResourceType(), action, segments, result);
            }
            case TOPIC -> {
                result.put(ResourceType.TOPIC, result.get(ResourceType.PROJECT) + ":" + segments.remove(0));
                yield result;
            }
            case SUBSCRIPTION -> {
                result.put(ResourceType.SUBSCRIPTION, result.get(ResourceType.PROJECT) + ":" + segments.remove(0));
                yield result;
            }
        };
    }

    private List<String> getRolesForSubject(String subject, String resourceId) {
        return configuration.getRoleBindings()
                .getOrDefault(resourceId, new HashMap<>())
                .getOrDefault(subject, new ArrayList<>());
    }

    private boolean isAuthorizedInternal(String subject, ResourceAction action, String resourceId) {
        log.debug(
                "Checking authorization for subject [{}] and action [{}] on resource [{}]", subject, action,
                resourceId
        );
        return getRolesForSubject(subject, resourceId).stream()
                .anyMatch(role -> doesActionBelongToRole(subject, role, action));
    }

    private boolean doesActionBelongToRole(String subject, String roleId, ResourceAction action) {
        log.debug("Evaluating action [{}] for subject [{}] against role [{}]", action, subject, roleId);
        boolean matching = configuration.getRoles().getOrDefault(roleId, new ArrayList<>()).contains(action);
        if (matching) {
            log.debug("Successfully matched action [{}] for subject [{}] against role [{}]", action, subject, roleId);
        }
        return matching;
    }
}
