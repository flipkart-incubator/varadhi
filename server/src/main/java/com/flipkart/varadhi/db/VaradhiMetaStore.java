package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.entities.auth.RoleBindingNode;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.RoleBindingMetaStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.flipkart.varadhi.db.ZNode.*;
import static com.flipkart.varadhi.entities.MetaStoreEntity.NAME_SEPARATOR;


@Slf4j
public class VaradhiMetaStore implements MetaStore, RoleBindingMetaStore {
    private final ZKMetaStore zkMetaStore;

    public VaradhiMetaStore(CuratorFramework zkCurator) {
        this.zkMetaStore = new ZKMetaStore(zkCurator);
        ensureEntityTypePathExists();
    }

    private void ensureEntityTypePathExists() {
        ensureEntityTypePathExists(ORG);
        ensureEntityTypePathExists(TEAM);
        ensureEntityTypePathExists(PROJECT);
        ensureEntityTypePathExists(TOPIC_RESOURCE);
        ensureEntityTypePathExists(VARADHI_TOPIC);
        ensureEntityTypePathExists(ROLE_BINDING);
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
    public List<String> getVaradhiTopicNames(String projectName) {
        String projectPrefixOfTopicName = projectName + NAME_SEPARATOR;
        ZNode znode = ZNode.OfEntityType(VARADHI_TOPIC);
        return zkMetaStore.listChildren(znode)
                .stream()
                .filter(name -> name.contains(projectPrefixOfTopicName))
                .collect(Collectors.toList());
    }

    @Override
    public void createVaradhiTopic(VaradhiTopic varadhiTopic) {
        ZNode znode = ZNode.OfVaradhiTopic(varadhiTopic.getName());
        zkMetaStore.createZNodeWithData(znode, varadhiTopic);
    }

    @Override
    public boolean checkVaradhiTopicExists(String varadhiTopicName) {
        try {
            getVaradhiTopic(varadhiTopicName);
            return true;
        } catch (ResourceNotFoundException ex) {
            log.warn("VaradhiTopic:{} does not exist.", varadhiTopicName);
        }
        return false;
    }

    @Override
    public VaradhiTopic getVaradhiTopic(String varadhiTopicName) throws ResourceNotFoundException {
        ZNode znode = ZNode.OfVaradhiTopic(varadhiTopicName);
        return zkMetaStore.getZNodeDataAsPojo(znode, VaradhiTopic.class);
    }

    @Override
    public void deleteVaradhiTopic(String varadhiTopicName) {
        ZNode znode = ZNode.OfVaradhiTopic(varadhiTopicName);
        zkMetaStore.deleteZNode(znode);
    }

    @Override
    public List<RoleBindingNode> getRoleBindingNodes() {
        ZNode znode = ZNode.OfEntityType(ROLE_BINDING);
        return zkMetaStore.listChildren(znode).stream().map(this::findRoleBindingNode).toList();
    }

    @Override
    public RoleBindingNode findRoleBindingNode(String resourceIdWithType) {
        ZNode znode = ZNode.OfKind(ROLE_BINDING, resourceIdWithType);
        return zkMetaStore.getZNodeDataAsPojo(znode, RoleBindingNode.class);
    }

    @Override
    public RoleBindingNode getRoleBindingNode(ResourceType resourceType, String resourceId) {
        ZNode znode = ZNode.OfIAMPolicy(resourceType, resourceId);
        return zkMetaStore.getZNodeDataAsPojo(znode, RoleBindingNode.class);
    }

    @Override
    public void createRoleBindingNode(RoleBindingNode node) {
        ZNode znode = ZNode.OfIAMPolicy(node.getResourceType(), node.getResourceId());
        zkMetaStore.createZNodeWithData(znode, node);
    }

    @Override
    public boolean isRoleBindingPresent(ResourceType resourceType, String resourceId) {
        ZNode znode = ZNode.OfIAMPolicy(resourceType, resourceId);
        return zkMetaStore.zkPathExist(znode);
    }

    @Override
    public int updateRoleBindingNode(RoleBindingNode node) {
        ZNode znode = ZNode.OfIAMPolicy(node.getResourceType(), node.getResourceId());
        return zkMetaStore.updateZNodeWithData(znode, node);
    }

    @Override
    public void deleteRoleBindingNode(ResourceType resourceType, String resourceId) {
        ZNode znode = ZNode.OfIAMPolicy(resourceType, resourceId);
        zkMetaStore.deleteZNode(znode);
    }
}
