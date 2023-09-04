package com.flipkart.varadhi.auth;

import com.flipkart.varadhi.entities.UserContext;
import com.flipkart.varadhi.exceptions.InvalidConfigException;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.flipkart.varadhi.auth.ResourceAction.ORG_CREATE;

public class DefaultAuthorizationProvider implements AuthorizationProvider {
    private DefaultAuthorizationConfiguration configuration;

    @Override
    public Future<Boolean> init(JsonObject configuration) {
        var def = new DefaultAuthorizationConfiguration(
                Map.of("org.admin", List.of(ORG_CREATE)),
                Map.of("flipkart", Map.of("aayush.gupta", List.of("org.admin"))));
        var res = JsonObject.mapFrom(def);
        this.configuration = configuration.mapTo(DefaultAuthorizationConfiguration.class);
        return Future.succeededFuture(true);
    }

    /**
     * Checks if the user {@code UserContext} is authorized to perform action {@code ResourceAction} on resource path.
     *
     * @param userContext information on the user, contains the user identifier
     * @param action      the action being performed by the user which is to be authorized
     * @param resource    the path of the resource on which the action is to be authorized
     *
     * @return {@code Future<Boolean>} a future result expressing True/False decision
     */
    @Override
    public Future<Boolean> isAuthorized(UserContext userContext, ResourceAction action, String resource) {
        // parse the resource path based on the action and return the final resourceIDs for each resourceType
        Map<ResourceType, String> resourceTypeToResourceId = getResourceIds(action, resource);
        return Future.succeededFuture(resourceTypeToResourceId.entrySet().stream()
                .anyMatch(entry -> isAuthorizedInternal(userContext.getSubject(), action, entry.getValue())));
    }

    private Map<ResourceType, String> getResourceIds(ResourceAction action, String resource) {
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
            case TENANT -> throw new InvalidConfigException("tenant resource not supported for authorization");
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
        return getRolesForSubject(subject, resourceId).stream()
                .anyMatch(role -> doesActionBelongToRole(role, action));
    }

    private boolean doesActionBelongToRole(String roleId, ResourceAction action) {
        return configuration.getRoles().getOrDefault(roleId, new ArrayList<>()).contains(action);
    }
}
