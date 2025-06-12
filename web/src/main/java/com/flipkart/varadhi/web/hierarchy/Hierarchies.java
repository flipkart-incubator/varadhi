package com.flipkart.varadhi.web.hierarchy;

import com.flipkart.varadhi.entities.Project;

import java.util.HashMap;
import java.util.Map;

import static com.flipkart.varadhi.common.Constants.Tags.*;

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


    public record ProjectHierarchy(Project project) implements ResourceHierarchy {

        @Override
        public String getResourcePath() {
            return String.format("/%s/%s/%s", project.getOrg(), project.getTeam(), project.getName());
        }

        @Override
        public Map<String, String> getAttributes() {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(TAG_ORG, project.getOrg());
            attributes.put(TAG_TEAM, project.getTeam());
            attributes.put(TAG_PROJECT, project.getName());
            return attributes;
        }
    }


    public record TopicHierarchy(Project project, String topic) implements ResourceHierarchy {

        @Override
        public String getResourcePath() {
            return String.format("/%s/%s/%s/%s", project.getOrg(), project.getTeam(), project.getName(), topic);
        }

        @Override
        public Map<String, String> getAttributes() {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(TAG_ORG, project.getOrg());
            attributes.put(TAG_TEAM, project.getTeam());
            attributes.put(TAG_PROJECT, project.getName());
            attributes.put(TAG_TOPIC, topic);
            return attributes;
        }
    }


    public record SubscriptionHierarchy(Project project, String subscription) implements ResourceHierarchy {

        @Override
        public String getResourcePath() {
            return String.format("/%s/%s/%s/%s", project.getOrg(), project.getTeam(), project.getName(), subscription);
        }

        @Override
        public Map<String, String> getAttributes() {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(TAG_ORG, project.getOrg());
            attributes.put(TAG_TEAM, project.getTeam());
            attributes.put(TAG_PROJECT, project.getName());
            attributes.put(TAG_SUBSCRIPTION, subscription);
            return attributes;
        }
    }


    public record IamPolicyHierarchy(ResourceHierarchy hierarchy) implements ResourceHierarchy {
        @Override
        public String getResourcePath() {
            return hierarchy.getResourcePath();
        }

        @Override
        public Map<String, String> getAttributes() {
            return hierarchy.getAttributes();
        }
    }
}
