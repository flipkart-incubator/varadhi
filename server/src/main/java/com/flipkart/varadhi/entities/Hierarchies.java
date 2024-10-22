package com.flipkart.varadhi.entities;

import com.flipkart.varadhi.entities.auth.ResourceType;

import java.util.HashMap;
import java.util.Map;

import static com.flipkart.varadhi.Constants.Tags.*;

public class Hierarchies {
    public record RootHierarchy() implements ResourceHierarchy {
        @Override
        public String getResourcePath() {
            return getResourcePath(ResourceType.ROOT);
        }

        @Override
        public String getResourcePath(ResourceType type) {
            if (type.equals(ResourceType.ROOT)) {
                return "/";
            } else {
                throw new IllegalArgumentException("Invalid Resource type %s for Root path.".formatted(type));
            }
        }

        @Override
        public Map<String, String> getAttributes() {
            return new HashMap<>();
        }
    }

    public record OrgHierarchy(String org) implements ResourceHierarchy {

        @Override
        public String getResourcePath() {
            return getResourcePath(ResourceType.ORG);
        }

        @Override
        public String getResourcePath(ResourceType type) {
            if (type.equals(ResourceType.ORG)) {
                return "/" + org;
            } else {
                throw new IllegalArgumentException("Invalid Resource type %s for Org path.".formatted(type));
            }
        }

        @Override
        public Map<String, String> getAttributes() {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(TAG_ORG, org);
            return attributes;
        }
    }

    public record TeamHierarchy(String org, String team) implements ResourceHierarchy {

        @Override
        public String getResourcePath() {
            return getResourcePath(ResourceType.TEAM);
        }

        @Override
        public String getResourcePath(ResourceType type) {
            if (type.equals(ResourceType.TEAM)) {
                return String.format("/%s/%s", org, team);
            } else {
                throw new IllegalArgumentException("Invalid Resource type %s for Team path.".formatted(type));
            }
        }

        @Override
        public Map<String, String> getAttributes() {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(TAG_ORG, org);
            attributes.put(TAG_TEAM, team);
            return attributes;
        }
    }

    public record ProjectHierarchy(String org, String team, String project) implements ResourceHierarchy {

        @Override
        public String getResourcePath() {
            return getResourcePath(ResourceType.PROJECT);
        }

        @Override
        public String getResourcePath(ResourceType type) {
            if (type.equals(ResourceType.PROJECT)) {
                return String.format("/%s/%s/%s", org, team, project);
            } else {
                throw new IllegalArgumentException("Invalid Resource type %s for Project path.".formatted(type));
            }
        }

        @Override
        public Map<String, String> getAttributes() {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(TAG_ORG, org);
            attributes.put(TAG_TEAM, team);
            attributes.put(TAG_PROJECT, project);
            return attributes;
        }
    }

    public record TopicHierarchy(String org, String team, String project, String topic) implements ResourceHierarchy {

        @Override
        public String getResourcePath() {
            return getResourcePath(ResourceType.TOPIC);
        }

        @Override
        public String getResourcePath(ResourceType type) {
            if (type.equals(ResourceType.TOPIC)) {
                return String.format("/%s/%s/%s/%s", org, team, project, topic);
            } else {
                throw new IllegalArgumentException("Invalid Resource type %s for Topic path.".formatted(type));
            }
        }

        @Override
        public Map<String, String> getAttributes() {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(TAG_ORG, org);
            attributes.put(TAG_TEAM, team);
            attributes.put(TAG_PROJECT, project);
            attributes.put(TAG_TOPIC, topic);
            return attributes;
        }
    }

    public record SubscriptionHierarchy(String org, String team, String project, String subscription,
                                        TopicHierarchy topicHierarchy) implements ResourceHierarchy {

        @Override
        public String getResourcePath() {
            return getResourcePath(ResourceType.SUBSCRIPTION);
        }

        @Override
        public String getResourcePath(ResourceType type) {
            if (type.equals(ResourceType.SUBSCRIPTION)) {
                return String.format("/%s/%s/%s/%s", org, team, project, subscription);
            } else if (type.equals(ResourceType.TOPIC)) {
                return topicHierarchy.getResourcePath(ResourceType.TOPIC);
            } else {
                throw new IllegalArgumentException("Invalid Resource type %s for Subscription path.".formatted(type));
            }
        }

        @Override
        public Map<String, String> getAttributes() {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(TAG_ORG, org);
            attributes.put(TAG_TEAM, team);
            attributes.put(TAG_TOPIC, topicHierarchy.topic);
            attributes.put(TAG_PROJECT, project);
            attributes.put(TAG_SUBSCRIPTION, subscription);
            return attributes;
        }
    }

    public record IamPolicyHierarchy(String resourceType, String resourceName) implements ResourceHierarchy {
        @Override
        public String getResourcePath() {
            return getResourcePath(ResourceType.IAM_POLICY);
        }
        @Override
        public String getResourcePath(ResourceType type) {
            if (type.equals(ResourceType.IAM_POLICY)) {
                return String.format("/%s/%s", resourceType, resourceName);
            } else {
                throw new IllegalArgumentException("Invalid Resource type %s for IAM Policy path.".formatted(type));
            }
        }

        @Override
        public Map<String, String> getAttributes() {
            Map<String, String> attributes = new HashMap<>();
            attributes.put("resource_type", resourceType);
            attributes.put("resource", resourceName);
            return attributes;
        }
    }
}
