package com.flipkart.varadhi.entities;

import java.util.HashMap;
import java.util.Map;

import static com.flipkart.varadhi.Constants.Tags.*;

public class Hierarchies {
    public record RootHierarchy() implements ResourceHierarchy {
        @Override
        public String getResourcePath() {
            return "/";
        }

        @Override
        public Map<String, String> getAttributes() {
            return new HashMap<>();
        }
    }

    public record OrgHierarchy(String org) implements ResourceHierarchy {
        @Override
        public String getResourcePath() {
            return "/" + org;
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
            return String.format("/%s/%s", org, team);
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
            return String.format("/%s/%s/%s", org, team, project);
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
            return String.format("/%s/%s/%s/%s", org, team, project, topic);
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

    public record SubscriptionHierarchy(String org, String team, String project, String subscription) implements ResourceHierarchy {
        @Override
        public String getResourcePath() {
            return String.format("/%s/%s/%s/%s", org, team, project, subscription);
        }

        @Override
        public Map<String, String> getAttributes() {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(TAG_ORG, org);
            attributes.put(TAG_TEAM, team);
            attributes.put(TAG_PROJECT, project);
            attributes.put(TAG_TOPIC, subscription);
            return attributes;
        }
    }

    public record IamPolicyHierarchy(String resourceType, String resourceName) implements ResourceHierarchy {
        @Override
        public String getResourcePath() {
            return String.format("/%s/%s", resourceType, resourceName);
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
