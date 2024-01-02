package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.auth.ResourceType;
import lombok.Getter;

@Getter
public class ZNode {
    public static final ZNodeKind ORG = new ZNodeKind("Org");
    public static final ZNodeKind TEAM = new ZNodeKind("Team");
    public static final ZNodeKind PROJECT = new ZNodeKind("Project");
    public static final ZNodeKind VARADHI_TOPIC = new ZNodeKind("VaradhiTopic");
    public static final ZNodeKind TOPIC_RESOURCE = new ZNodeKind("TopicResource");
    public static final ZNodeKind ROLE_BINDING = new ZNodeKind("RoleBinding");
    public static final String BASE_PATH = "/varadhi/entities";
    public static final String RESOURCE_NAME_SEPARATOR = ":";
    public static final String ZK_PATH_SEPARATOR = "/";

    private final String path;
    private final String kind;
    private final String name;

    private ZNode(ZNodeKind znodeKind) {
        this.name = znodeKind.getKind();
        this.kind = znodeKind.getKind();
        this.path = String.join(ZK_PATH_SEPARATOR, BASE_PATH, znodeKind.getKind());
    }

    private ZNode(ZNodeKind znodeKind, String entityName) {
        this.name = entityName;
        this.kind = znodeKind.getKind();
        this.path = String.join(ZK_PATH_SEPARATOR, BASE_PATH, znodeKind.getKind(), entityName);
    }

    private ZNode(ZNodeKind znodeKind, String parent, String entityName) {
        this.name = entityName;
        this.kind = znodeKind.getKind();
        this.path = String.join(
                ZK_PATH_SEPARATOR, BASE_PATH, znodeKind.getKind(), getResourceFQDN(parent, entityName));
    }

    public static String getResourceFQDN(String parentName, String resourceName) {
        return String.join(RESOURCE_NAME_SEPARATOR, parentName, resourceName);
    }

    public static ZNode OfOrg(String orgName) {
        return new ZNode(ORG, orgName);
    }

    public static ZNode OfTeam(String orgName, String teamName) {
        return new ZNode(TEAM, orgName, teamName);
    }

    public static ZNode OfProject(String projectName) {
        return new ZNode(PROJECT, projectName);
    }

    public static ZNode OfVaradhiTopic(String varadhiTopicName) {
        return new ZNode(VARADHI_TOPIC, varadhiTopicName);
    }

    public static ZNode OfTopicResource(String projectName, String topicResourceName) {
        return new ZNode(TOPIC_RESOURCE, projectName, topicResourceName);
    }

    public static ZNode OfIAMPolicy(ResourceType resourceType, String resourceId) {
        return new ZNode(ROLE_BINDING, resourceType.toString(), resourceId);
    }

    public static ZNode OfKind(ZNodeKind zNodeKind, String name) {
        return new ZNode(zNodeKind, name);
    }

    public static ZNode OfEntityType(ZNodeKind kind) {
        return new ZNode(kind);
    }

}
