package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.auth.IamPolicyRecord;
import com.flipkart.varadhi.spi.db.IamPolicyMetaStore;
import com.flipkart.varadhi.spi.db.MetaStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.varadhi.db.ZNode.*;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR;


@Slf4j
public class VaradhiMetaStore implements MetaStore, IamPolicyMetaStore {
    private final ZKMetaStore zkMetaStore;

    public VaradhiMetaStore(CuratorFramework zkCurator) {
        this.zkMetaStore = new ZKMetaStore(zkCurator);
        ensureEntityTypePathExists();
    }

    private void ensureEntityTypePathExists() {
        ensureEntityTypePathExists(ORG);
        ensureEntityTypePathExists(TEAM);
        ensureEntityTypePathExists(PROJECT);
        ensureEntityTypePathExists(TOPIC);
        ensureEntityTypePathExists(SUBSCRIPTION);
        ensureEntityTypePathExists(IAM_POLICY);
    }

    public void ensureEntityTypePathExists(ZNodeKind zNodeKind) {
        ZNode znode = ZNode.OfEntityType(zNodeKind);
        if (!zkMetaStore.zkPathExist(znode)) {
            zkMetaStore.createZNode(znode);
        }
    }

    @Override
    public void createOrg(Org org) {
        ZNode znode = ZNode.OfOrg(org.getName());
        zkMetaStore.createZNodeWithData(znode, org);
    }

    @Override
    public Org getOrg(String orgName) {
        ZNode znode = ZNode.OfOrg(orgName);
        return zkMetaStore.getZNodeDataAsPojo(znode, Org.class);
    }

    @Override
    public boolean checkOrgExists(String orgName) {
        ZNode znode = ZNode.OfOrg(orgName);
        return zkMetaStore.zkPathExist(znode);
    }

    @Override
    public void deleteOrg(String orgName) {
        ZNode znode = ZNode.OfOrg(orgName);
        zkMetaStore.deleteZNode(znode);
    }

    @Override
    public List<Org> getOrgs() {
        List<Org> orgs = new ArrayList<>();
        ZNode znode = ZNode.OfEntityType(ORG);
        zkMetaStore.listChildren(znode).forEach(orgName -> orgs.add(getOrg(orgName)));
        return orgs;
    }

    @Override
    public List<String> getTeamNames(String orgName) {
        List<String> teamNames = new ArrayList<>();
        String orgPrefixOfTeamName = orgName + RESOURCE_NAME_SEPARATOR;
        // This filters out incorrectly format entries too, but that should be ok.
        ZNode znode = ZNode.OfEntityType(TEAM);
        zkMetaStore.listChildren(znode).forEach(teamName -> {
                    if (teamName.startsWith(orgPrefixOfTeamName)) {
                        String[] splits = teamName.split(RESOURCE_NAME_SEPARATOR);
                        teamNames.add(splits[1]);
                    }
                }
        );
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
        ZNode znode = ZNode.OfTeam(team.getOrg(), team.getName());
        zkMetaStore.createZNodeWithData(znode, team);
    }

    @Override
    public Team getTeam(String teamName, String orgName) {
        ZNode znode = ZNode.OfTeam(orgName, teamName);
        return zkMetaStore.getZNodeDataAsPojo(znode, Team.class);
    }

    @Override
    public boolean checkTeamExists(String teamName, String orgName) {
        ZNode znode = ZNode.OfTeam(orgName, teamName);
        return zkMetaStore.zkPathExist(znode);
    }

    @Override
    public void deleteTeam(String teamName, String orgName) {
        ZNode znode = ZNode.OfTeam(orgName, teamName);
        zkMetaStore.deleteZNode(znode);
    }

    @Override
    public List<Project> getProjects(String teamName, String orgName) {
        List<Project> projects = new ArrayList<>();
        // This filters out incorrectly format entries too, but that should be ok.
        ZNode znode = ZNode.OfEntityType(PROJECT);
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
        ZNode znode = ZNode.OfProject(project.getName());
        zkMetaStore.createZNodeWithData(znode, project);
    }

    @Override
    public Project getProject(String projectName) {
        ZNode znode = ZNode.OfProject(projectName);
        return zkMetaStore.getZNodeDataAsPojo(znode, Project.class);
    }

    @Override
    public boolean checkProjectExists(String projectName) {
        ZNode znode = ZNode.OfProject(projectName);
        return zkMetaStore.zkPathExist(znode);
    }

    @Override
    public void deleteProject(String projectName) {
        ZNode znode = ZNode.OfProject(projectName);
        zkMetaStore.deleteZNode(znode);
    }

    @Override
    public int updateProject(Project project) {
        ZNode znode = ZNode.OfProject(project.getName());
        return zkMetaStore.updateZNodeWithData(znode, project);
    }


    @Override
    public List<String> getTopicNames(String projectName) {
        String projectPrefixOfTopicName = projectName + NAME_SEPARATOR;
        ZNode znode = ZNode.OfEntityType(TOPIC);
        return zkMetaStore.listChildren(znode)
                .stream()
                .filter(name -> name.contains(projectPrefixOfTopicName))
                .toList();
    }

    @Override
    public void createTopic(VaradhiTopic topic) {
        ZNode znode = ZNode.OfTopic(topic.getName());
        zkMetaStore.createZNodeWithData(znode, topic);
    }

    @Override
    public boolean checkTopicExists(String topicName) {
        ZNode znode = ZNode.OfTopic(topicName);
        return zkMetaStore.zkPathExist(znode);
    }

    @Override
    public VaradhiTopic getTopic(String topicName) {
        ZNode znode = ZNode.OfTopic(topicName);
        return zkMetaStore.getZNodeDataAsPojo(znode, VaradhiTopic.class);
    }

    @Override
    public void deleteTopic(String topicName) {
        ZNode znode = ZNode.OfTopic(topicName);
        zkMetaStore.deleteZNode(znode);
    }

    @Override
    public List<String> getAllSubscriptionNames() {
        ZNode znode = ZNode.OfEntityType(SUBSCRIPTION);
        return zkMetaStore.listChildren(znode)
                .stream()
                .toList();
    }

    @Override
    public List<String> getSubscriptionNames(String projectName) {
        String projectPrefix = projectName + NAME_SEPARATOR;
        ZNode znode = ZNode.OfEntityType(SUBSCRIPTION);
        return zkMetaStore.listChildren(znode)
                .stream()
                .filter(name -> name.contains(projectPrefix))
                .toList();
    }

    @Override
    public void createSubscription(VaradhiSubscription subscription) {
        ZNode znode = ZNode.ofSubscription(subscription.getName());
        zkMetaStore.createZNodeWithData(znode, subscription);
    }

    @Override
    public VaradhiSubscription getSubscription(String subscriptionName) {
        ZNode znode = ZNode.ofSubscription(subscriptionName);
        return zkMetaStore.getZNodeDataAsPojo(znode, VaradhiSubscription.class);
    }

    @Override
    public int updateSubscription(VaradhiSubscription subscription) {
        ZNode znode = ZNode.ofSubscription(subscription.getName());
        return zkMetaStore.updateZNodeWithData(znode, subscription);
    }

    @Override
    public boolean checkSubscriptionExists(String subscriptionName) {
        ZNode znode = ZNode.ofSubscription(subscriptionName);
        return zkMetaStore.zkPathExist(znode);
    }

    @Override
    public void deleteSubscription(String subscriptionName) {
        ZNode znode = ZNode.ofSubscription(subscriptionName);
        zkMetaStore.deleteZNode(znode);
    }

    @Override
    public IamPolicyRecord getIamPolicyRecord(String authResourceId) {
        ZNode znode = ZNode.OfIamPolicy(authResourceId);
        return zkMetaStore.getZNodeDataAsPojo(znode, IamPolicyRecord.class);
    }

    @Override
    public void createIamPolicyRecord(IamPolicyRecord node) {
        ZNode znode = ZNode.OfIamPolicy(node.getAuthResourceId());
        zkMetaStore.createZNodeWithData(znode, node);
    }

    @Override
    public boolean isIamPolicyRecordPresent(String authResourceId) {
        ZNode znode = ZNode.OfIamPolicy(authResourceId);
        return zkMetaStore.zkPathExist(znode);
    }

    @Override
    public int updateIamPolicyRecord(IamPolicyRecord node) {
        ZNode znode = ZNode.OfIamPolicy(node.getAuthResourceId());
        return zkMetaStore.updateZNodeWithData(znode, node);
    }

    @Override
    public void deleteIamPolicyRecord(String authResourceId) {
        ZNode znode = ZNode.OfIamPolicy(authResourceId);
        zkMetaStore.deleteZNode(znode);
    }
}
