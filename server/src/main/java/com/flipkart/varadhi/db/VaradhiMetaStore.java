package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.auth.IamPolicyRecord;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.spi.db.EventCallback;
import com.flipkart.varadhi.spi.db.IamPolicyMetaStore;
import com.flipkart.varadhi.spi.db.MetaStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.varadhi.db.ZNode.EVENT;
import static com.flipkart.varadhi.db.ZNode.IAM_POLICY;
import static com.flipkart.varadhi.db.ZNode.ORG;
import static com.flipkart.varadhi.db.ZNode.PROJECT;
import static com.flipkart.varadhi.db.ZNode.RESOURCE_NAME_SEPARATOR;
import static com.flipkart.varadhi.db.ZNode.SUBSCRIPTION;
import static com.flipkart.varadhi.db.ZNode.TEAM;
import static com.flipkart.varadhi.db.ZNode.TOPIC;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR;

@Slf4j
public class VaradhiMetaStore implements MetaStore, IamPolicyMetaStore {
    private final ZKMetaStore zkMetaStore;

    public VaradhiMetaStore(CuratorFramework zkCurator) {
        this.zkMetaStore = new ZKMetaStore(zkCurator);
        ensureEntityTypePathExists();
    }

    private void ensureEntityTypePathExists() {
        zkMetaStore.createZNode(ZNode.ofEntityType(ORG));
        zkMetaStore.createZNode(ZNode.ofEntityType(TEAM));
        zkMetaStore.createZNode(ZNode.ofEntityType(PROJECT));
        zkMetaStore.createZNode(ZNode.ofEntityType(TOPIC));
        zkMetaStore.createZNode(ZNode.ofEntityType(SUBSCRIPTION));
        zkMetaStore.createZNode(ZNode.ofEntityType(IAM_POLICY));
        zkMetaStore.createZNode(ZNode.ofEntityType(EVENT));
    }

    @Override
    public boolean registerEventListener(EventCallback callback) {
        return zkMetaStore.registerEventListener(callback);
    }

    @Override
    public void createOrg(Org org) {
        ZNode znode = ZNode.ofOrg(org.getName());
        zkMetaStore.createZNodeWithData(znode, org);
    }

    @Override
    public Org getOrg(String orgName) {
        ZNode znode = ZNode.ofOrg(orgName);
        return zkMetaStore.getZNodeDataAsPojo(znode, Org.class);
    }

    @Override
    public boolean checkOrgExists(String orgName) {
        ZNode znode = ZNode.ofOrg(orgName);
        return zkMetaStore.zkPathExist(znode);
    }

    @Override
    public void deleteOrg(String orgName) {
        ZNode znode = ZNode.ofOrg(orgName);
        zkMetaStore.deleteZNode(znode);
    }

    @Override
    public List<Org> getOrgs() {
        List<Org> orgs = new ArrayList<>();
        ZNode znode = ZNode.ofEntityType(ORG);
        zkMetaStore.listChildren(znode).forEach(orgName -> orgs.add(getOrg(orgName)));
        return orgs;
    }

    @Override
    public List<String> getTeamNames(String orgName) {
        List<String> teamNames = new ArrayList<>();
        String orgPrefixOfTeamName = orgName + RESOURCE_NAME_SEPARATOR;
        // This filters out incorrectly format entries too, but that should be ok.
        ZNode znode = ZNode.ofEntityType(TEAM);
        zkMetaStore.listChildren(znode).forEach(teamName -> {
            if (teamName.startsWith(orgPrefixOfTeamName)) {
                String[] splits = teamName.split(RESOURCE_NAME_SEPARATOR);
                teamNames.add(splits[1]);
            }
        });
        return teamNames;
    }

    @Override
    public List<Team> getTeams(String orgName) {
        List<Team> teams = new ArrayList<>();
        getTeamNames(orgName).forEach(teamName -> teams.add(getTeam(teamName, orgName)));
        return teams;
    }

    @Override
    public void createTeam(Team team) {
        ZNode znode = ZNode.ofTeam(team.getOrg(), team.getName());
        zkMetaStore.createZNodeWithData(znode, team);
    }

    @Override
    public Team getTeam(String teamName, String orgName) {
        ZNode znode = ZNode.ofTeam(orgName, teamName);
        return zkMetaStore.getZNodeDataAsPojo(znode, Team.class);
    }

    @Override
    public boolean checkTeamExists(String teamName, String orgName) {
        ZNode znode = ZNode.ofTeam(orgName, teamName);
        return zkMetaStore.zkPathExist(znode);
    }

    @Override
    public void deleteTeam(String teamName, String orgName) {
        ZNode znode = ZNode.ofTeam(orgName, teamName);
        zkMetaStore.deleteZNode(znode);
    }

    @Override
    public List<Project> getProjects(String teamName, String orgName) {
        List<Project> projects = new ArrayList<>();
        // This filters out incorrectly format entries too, but that should be ok.
        ZNode znode = ZNode.ofEntityType(PROJECT);
        zkMetaStore.listChildren(znode).forEach(projectName -> {
            Project project = getProject(projectName);
            if (project.getOrg().equals(orgName) && project.getTeam().equals(teamName)) {
                projects.add(project);
            }
        });
        return projects;
    }

    @Override
    public void createProject(Project project) {
        ZNode znode = ZNode.ofProject(project.getName());
        zkMetaStore.createTrackedResource(znode, project, ResourceType.PROJECT);
    }

    @Override
    public Project getProject(String projectName) {
        ZNode znode = ZNode.ofProject(projectName);
        return zkMetaStore.getZNodeDataAsPojo(znode, Project.class);
    }

    @Override
    public boolean checkProjectExists(String projectName) {
        ZNode znode = ZNode.ofProject(projectName);
        return zkMetaStore.zkPathExist(znode);
    }

    @Override
    public void deleteProject(String projectName) {
        ZNode znode = ZNode.ofProject(projectName);
        zkMetaStore.deleteTrackedResource(znode, ResourceType.PROJECT);
    }

    @Override
    public void updateProject(Project project) {
        ZNode znode = ZNode.ofProject(project.getName());
        zkMetaStore.updateTrackedResource(znode, project, ResourceType.PROJECT);
    }

    @Override
    public List<String> getTopicNames(String projectName) {
        String projectPrefixOfTopicName = projectName + NAME_SEPARATOR;
        ZNode znode = ZNode.ofEntityType(TOPIC);
        return zkMetaStore.listChildren(znode)
                          .stream()
                          .filter(name -> name.contains(projectPrefixOfTopicName))
                          .toList();
    }

    @Override
    public void createTopic(VaradhiTopic topic) {
        ZNode znode = ZNode.ofTopic(topic.getName());
        zkMetaStore.createTrackedResource(znode, topic, ResourceType.TOPIC);
    }

    @Override
    public boolean checkTopicExists(String topicName) {
        ZNode znode = ZNode.ofTopic(topicName);
        return zkMetaStore.zkPathExist(znode);
    }

    @Override
    public VaradhiTopic getTopic(String topicName) {
        ZNode znode = ZNode.ofTopic(topicName);
        return zkMetaStore.getZNodeDataAsPojo(znode, VaradhiTopic.class);
    }

    @Override
    public void deleteTopic(String topicName) {
        ZNode znode = ZNode.ofTopic(topicName);
        zkMetaStore.deleteTrackedResource(znode, ResourceType.TOPIC);
    }

    @Override
    public void updateTopic(VaradhiTopic topic) {
        ZNode znode = ZNode.ofTopic(topic.getName());
        zkMetaStore.updateTrackedResource(znode, topic, ResourceType.TOPIC);
    }

    @Override
    public List<String> getAllSubscriptionNames() {
        ZNode znode = ZNode.ofEntityType(SUBSCRIPTION);
        return zkMetaStore.listChildren(znode).stream().toList();
    }

    @Override
    public List<String> getSubscriptionNames(String projectName) {
        String projectPrefix = projectName + NAME_SEPARATOR;
        ZNode znode = ZNode.ofEntityType(SUBSCRIPTION);
        return zkMetaStore.listChildren(znode).stream().filter(name -> name.contains(projectPrefix)).toList();
    }

    @Override
    public void createSubscription(VaradhiSubscription subscription) {
        ZNode znode = ZNode.ofSubscription(subscription.getName());
        zkMetaStore.createTrackedResource(znode, subscription, ResourceType.SUBSCRIPTION);
    }

    @Override
    public VaradhiSubscription getSubscription(String subscriptionName) {
        ZNode znode = ZNode.ofSubscription(subscriptionName);
        return zkMetaStore.getZNodeDataAsPojo(znode, VaradhiSubscription.class);
    }

    @Override
    public void updateSubscription(VaradhiSubscription subscription) {
        ZNode znode = ZNode.ofSubscription(subscription.getName());
        zkMetaStore.updateTrackedResource(znode, subscription, ResourceType.SUBSCRIPTION);
    }

    @Override
    public boolean checkSubscriptionExists(String subscriptionName) {
        ZNode znode = ZNode.ofSubscription(subscriptionName);
        return zkMetaStore.zkPathExist(znode);
    }

    @Override
    public void deleteSubscription(String subscriptionName) {
        ZNode znode = ZNode.ofSubscription(subscriptionName);
        zkMetaStore.deleteTrackedResource(znode, ResourceType.SUBSCRIPTION);
    }

    @Override
    public IamPolicyRecord getIamPolicyRecord(String authResourceId) {
        ZNode znode = ZNode.ofIamPolicy(authResourceId);
        return zkMetaStore.getZNodeDataAsPojo(znode, IamPolicyRecord.class);
    }

    @Override
    public void createIamPolicyRecord(IamPolicyRecord iamPolicyRecord) {
        ZNode znode = ZNode.ofIamPolicy(iamPolicyRecord.getName());
        zkMetaStore.createZNodeWithData(znode, iamPolicyRecord);
    }

    @Override
    public boolean isIamPolicyRecordPresent(String authResourceId) {
        ZNode znode = ZNode.ofIamPolicy(authResourceId);
        return zkMetaStore.zkPathExist(znode);
    }

    @Override
    public void updateIamPolicyRecord(IamPolicyRecord iamPolicyRecord) {
        ZNode znode = ZNode.ofIamPolicy(iamPolicyRecord.getName());
        zkMetaStore.updateZNodeWithData(znode, iamPolicyRecord);
    }

    @Override
    public void deleteIamPolicyRecord(String authResourceId) {
        ZNode znode = ZNode.ofIamPolicy(authResourceId);
        zkMetaStore.deleteZNode(znode);
    }
}
