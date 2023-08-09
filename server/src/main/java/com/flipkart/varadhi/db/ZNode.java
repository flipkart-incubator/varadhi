package com.flipkart.varadhi.db;

import lombok.Getter;

@Getter
public class ZNode {
    public static final ZNodeKind ORG = new ZNodeKind("Org");
    public static final ZNodeKind TEAM = new ZNodeKind("Team");
    public static final ZNodeKind PROJECT = new ZNodeKind("Project");
    public static final ZNodeKind VARADHI_TOPIC = new ZNodeKind("VaradhiTopic");
    public static final ZNodeKind TOPIC_RESOURCE = new ZNodeKind("TopicResource");
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

    private ZNode(String entityName, ZNodeKind znodeKind) {
        this.name = entityName;
        this.kind = znodeKind.getKind();
        this.path = String.join(ZK_PATH_SEPARATOR, BASE_PATH, znodeKind.getKind(), entityName);
    }

    private ZNode(String entityName, String parent, ZNodeKind znodeKind) {
        this.name = entityName;
        this.kind = znodeKind.getKind();
        this.path = String.join(
                ZK_PATH_SEPARATOR, BASE_PATH, znodeKind.getKind(), getResourceFQDN(parent, entityName));
    }

    public static String getResourceFQDN(String parentName, String resourceName) {
        return String.join(RESOURCE_NAME_SEPARATOR, parentName, resourceName);
    }

    public static ZNode OfOrg(String orgName) {
        return new ZNode(orgName, ORG);
    }

    public static ZNode OfTeam(String teamName, String orgName) {
        return new ZNode(teamName, orgName, TEAM);
    }

    public static ZNode OfProject(String projectName) {
        return new ZNode(projectName, PROJECT);
    }

    public static ZNode OfVaradhiTopic(String varadhiTopicName) {
        return new ZNode(varadhiTopicName, VARADHI_TOPIC);
    }

    public static ZNode OfTopicResource(String topicResourceName, String projectName) {
        return new ZNode(topicResourceName, projectName, TOPIC_RESOURCE);
    }

    public static ZNode OfKind(ZNodeKind zNodeKind, String name) {
        return new ZNode(name, zNodeKind);
    }

    public static ZNode OfEntityType(ZNodeKind kind) {
        return new ZNode(kind);
    }

}
