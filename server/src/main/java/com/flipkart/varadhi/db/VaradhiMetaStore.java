package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.MetaStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.flipkart.varadhi.Constants.NAME_SEPARATOR;
import static com.flipkart.varadhi.db.ZNode.*;


@Slf4j
public class VaradhiMetaStore implements MetaStore {
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
    public void createTopicResource(TopicResource resource) {
        ZNode znode = ZNode.OfTopicResource(resource.getProject(), resource.getName());
        zkMetaStore.createZNodeWithData(znode, resource);
    }

    @Override
    public boolean checkTopicResourceExists(String topicResourceName, String projectName) {
        ZNode znode = ZNode.OfTopicResource(projectName, topicResourceName);
        return zkMetaStore.zkPathExist(znode);
    }

    @Override
    public VaradhiTopic getVaradhiTopic(String topicResourceName, String projectName) {
        List<String> varadhiTopicNames = getVaradhiTopicNames(projectName);
        List<String> matchedVaradhiTopicNames = varadhiTopicNames.stream().filter(
                varadhiTopicName -> varadhiTopicName.contains(topicResourceName))
                .limit(1)
                .toList();
        if(matchedVaradhiTopicNames.isEmpty()) {
            throw new ResourceNotFoundException(String.format(
                    "Topic(%s) not found for the Project(%s).",
                    topicResourceName,
                    projectName
            ));
        }
        String varadhiTopicName = matchedVaradhiTopicNames.get(0);
        ZNode znode = ZNode.OfVaradhiTopic(varadhiTopicName);
        VaradhiTopic varadhiTopic = zkMetaStore.getZNodeDataAsPojo(znode, VaradhiTopic.class);
        return varadhiTopic;
    }

    @Override
    public List<String> getTopicResourceNames(String projectName) {
        List<String> topicResourceNames = new ArrayList<>();
        String projectPrefixOfVaradhiTopic = projectName + NAME_SEPARATOR;
        ZNode znode = ZNode.OfEntityType(VARADHI_TOPIC);
        zkMetaStore.listChildren(znode).forEach(varadhiTopic -> {
                    if (varadhiTopic.startsWith(projectPrefixOfVaradhiTopic)) {
                        String[] splits = varadhiTopic.split("\\.");
                        topicResourceNames.add(splits[1]);
                    }
                }
        );
        return topicResourceNames;
    }

    @Override
    public void deleteTopicResource(String topicResourceName, String projectName) {
        ZNode znode = ZNode.OfTopicResource(projectName, topicResourceName);
        zkMetaStore.deleteZNode(znode);
    }

    @Override
    public void createVaradhiTopic(VaradhiTopic varadhiTopic) {
        ZNode znode = ZNode.OfVaradhiTopic(varadhiTopic.getName());
        zkMetaStore.createZNodeWithData(znode, varadhiTopic);
    }

    @Override
    public boolean checkVaradhiTopicExists(String varadhiTopicName) {
        ZNode znode = ZNode.OfVaradhiTopic(varadhiTopicName);
        return zkMetaStore.zkPathExist(znode);
    }

    @Override
    public VaradhiTopic getVaradhiTopic(String varadhiTopicName) {
        ZNode znode = ZNode.OfVaradhiTopic(varadhiTopicName);
        return zkMetaStore.getZNodeDataAsPojo(znode, VaradhiTopic.class);
    }

    @Override
    public void deleteVaradhiTopic(String varadhiTopicName) {
        ZNode znode = ZNode.OfVaradhiTopic(varadhiTopicName);
        zkMetaStore.deleteZNode(znode);
    }

}
