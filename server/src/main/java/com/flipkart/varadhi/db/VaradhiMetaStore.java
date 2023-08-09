package com.flipkart.varadhi.db;

import com.flipkart.varadhi.ExceptionOrResult;
import com.flipkart.varadhi.entities.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.List;
import java.util.stream.Collectors;

import static com.flipkart.varadhi.db.ZNode.*;


@Slf4j
public class VaradhiMetaStore implements MetaStore {
    private final ZKMetaStore zkMetaStore;

    public VaradhiMetaStore(CuratorFramework zkCurator) {
        this.zkMetaStore = new ZKMetaStore(zkCurator);
        ensureEntityTypePathExists();
    }

    private void ensureEntityTypePathExists() {
        this.ensureEntityTypePathExists(ORG);
        this.ensureEntityTypePathExists(TEAM);
        this.ensureEntityTypePathExists(PROJECT);
        this.ensureEntityTypePathExists(TOPIC_RESOURCE);
        this.ensureEntityTypePathExists(VARADHI_TOPIC);
    }

    public void ensureEntityTypePathExists(ZNodeKind zNodeKind) {
        ZNode znode = ZNode.OfEntityType(zNodeKind);
        if (!this.zkMetaStore.zkPathExist(znode)) {
            this.zkMetaStore.createZNode(znode);
        }
    }

    @Override
    public void createOrg(Org org) {
        ZNode znode = ZNode.OfOrg(org.getName());
        this.zkMetaStore.createZNodeWithData(znode, org);
    }

    @Override
    public Org getOrg(String orgName) {
        ZNode znode = ZNode.OfOrg(orgName);
        return this.zkMetaStore.getZNodeDataAsPojo(znode, Org.class);
    }

    @Override
    public boolean checkOrgExists(String orgName) {
        ZNode znode = ZNode.OfOrg(orgName);
        return this.zkMetaStore.zkPathExist(znode);
    }

    @Override
    public void deleteOrg(String orgName) {
        ZNode znode = ZNode.OfOrg(orgName);
        this.zkMetaStore.deleteZNode(znode);
    }

    @Override
    public List<Org> getOrgs() {
        ZNode znode = ZNode.OfEntityType(ORG);
        return this.zkMetaStore.listChildren(znode)
                .stream()
                .map(name -> {
                    //TODO:: Discuss for Removing this handling. Discuss this exception handling may not be required if
                    // it can be assumed no changes are not made externally.
                    try {
                        return ExceptionOrResult.Result(getOrg(name));
                    } catch (Exception e) {
                        log.error("Failed to getOrg({}) Exception:{}", name, e);
                        return ExceptionOrResult.<Exception, Org>Failure(e);
                    }
                })
                .filter(ExceptionOrResult::hasResult)
                .map(ExceptionOrResult::getResult)
                .collect(Collectors.toList());
    }

    @Override
    public List<String> getTeamNames(String orgName) {
        String orgPrefixOfTeamName = orgName + RESOURCE_NAME_SEPARATOR;
        // This filters out incorrectly format entries too, but that should be ok.
        ZNode znode = ZNode.OfEntityType(TEAM);
        return this.zkMetaStore.listChildren(znode)
                .stream()
                .filter(name -> name.startsWith(orgPrefixOfTeamName))
                .map(name -> name.split(RESOURCE_NAME_SEPARATOR)[1])
                .collect(Collectors.toList());
    }

    @Override
    public List<Team> getTeams(String orgName) {
        String orgPrefixOfTeamName = orgName + RESOURCE_NAME_SEPARATOR;
        // This filters out incorrectly format entries too, but that should be ok.
        ZNode znode = ZNode.OfEntityType(TEAM);
        return this.zkMetaStore.listChildren(znode)
                .stream()
                .filter(name -> name.startsWith(orgPrefixOfTeamName))
                .map(name -> {
                    //TODO:: Remove this handling. Discuss this exception handling may not be required if
                    // it can be assumed no changes are not made externally.
                    try {
                        String[] splits = name.split(RESOURCE_NAME_SEPARATOR);
                        return ExceptionOrResult.Result(getTeam(splits[1], splits[0]));
                    } catch (Exception e) {
                        log.error("Failed to getTeam({}) Exception:{}", name, e);
                        return ExceptionOrResult.<Exception, Team>Failure(e);
                    }
                }).filter(ExceptionOrResult::hasResult)
                .map(ExceptionOrResult::getResult)
                .collect(Collectors.toList());
    }

    @Override
    public void createTeam(Team team) {
        ZNode znode = ZNode.OfTeam(team.getName(), team.getOrgName());
        this.zkMetaStore.createZNodeWithData(znode, team);
    }

    @Override
    public Team getTeam(String teamName, String orgName) {
        ZNode znode = ZNode.OfTeam(teamName, orgName);
        return this.zkMetaStore.getZNodeDataAsPojo(znode, Team.class);
    }

    @Override
    public boolean checkTeamExists(String teamName, String orgName) {
        ZNode znode = ZNode.OfTeam(teamName, orgName);
        return this.zkMetaStore.zkPathExist(znode);
    }

    @Override
    public void deleteTeam(String teamName, String orgName) {
        ZNode znode = ZNode.OfTeam(teamName, orgName);
        this.zkMetaStore.deleteZNode(znode);
    }

    @Override
    public List<Project> getProjects(String teamName, String orgName) {
        // This filters out incorrectly format entries too, but that should be ok.
        ZNode znode = ZNode.OfEntityType(PROJECT);
        return this.zkMetaStore.listChildren(znode)
                .stream()
                .map(name -> {
                    try {
                        return ExceptionOrResult.Result(getProject(name));
                    } catch (Exception e) {
                        log.error("Failed to getProject({}) Exception:{}", name, e);
                        return ExceptionOrResult.<Exception, Project>Failure(e);
                    }
                })
                .filter(ExceptionOrResult::hasResult)
                .map(ExceptionOrResult::getResult)
                .filter(project -> project.getOrgName().equals(orgName) && project.getTeamName().equals(teamName))
                .collect(Collectors.toList());
    }

    @Override
    public void createProject(Project project) {
        ZNode znode = ZNode.OfProject(project.getName());
        this.zkMetaStore.createZNodeWithData(znode, project);
    }

    @Override
    public Project getProject(String projectName) {
        ZNode znode = ZNode.OfProject(projectName);
        return this.zkMetaStore.getZNodeDataAsPojo(znode, Project.class);
    }

    @Override
    public boolean checkProjectExists(String projectName) {
        ZNode znode = ZNode.OfProject(projectName);
        return this.zkMetaStore.zkPathExist(znode);
    }

    @Override
    public void deleteProject(String projectName) {
        ZNode znode = ZNode.OfProject(projectName);
        this.zkMetaStore.deleteZNode(znode);
    }

    @Override
    public int updateProject(Project project) {
        ZNode znode = ZNode.OfProject(project.getName());
        return this.zkMetaStore.updateZNodeWithData(znode, project);
    }


    @Override
    public List<String> getVaradhiTopicNames(String projectName) {
        String projectPrefixOfTopicName = projectName + RESOURCE_NAME_SEPARATOR;
        ZNode znode = ZNode.OfEntityType(VARADHI_TOPIC);
        return this.zkMetaStore.listChildren(znode)
                .stream()
                .filter(name -> name.contains(projectPrefixOfTopicName))
                .map(name -> name.split(RESOURCE_NAME_SEPARATOR)[1])
                .collect(Collectors.toList());
    }

    @Override
    public void createTopicResource(TopicResource resource) {
        ZNode znode = ZNode.OfTopicResource(resource.getName(), resource.getProject());
        this.zkMetaStore.createZNodeWithData(znode, resource);
    }

    @Override
    public boolean checkTopicResourceExists(String topicResourceName, String projectName) {
        ZNode znode = ZNode.OfTopicResource(topicResourceName, projectName);
        return this.zkMetaStore.zkPathExist(znode);
    }

    @Override
    public TopicResource getTopicResource(String topicResourceName, String projectName) {
        ZNode znode = ZNode.OfTopicResource(topicResourceName, projectName);
        return this.zkMetaStore.getZNodeDataAsPojo(znode, TopicResource.class);
    }

    @Override
    public void deleteTopicResource(String topicResourceName, String projectName) {
        ZNode znode = ZNode.OfTopicResource(topicResourceName, projectName);
        this.zkMetaStore.deleteZNode(znode);
    }

    @Override
    public void createVaradhiTopic(VaradhiTopic varadhiTopic) {
        ZNode znode = ZNode.OfVaradhiTopic(varadhiTopic.getName());
        this.zkMetaStore.createZNodeWithData(znode, varadhiTopic);
    }

    @Override
    public boolean checkVaradhiTopicExists(String varadhiTopicName) {
        ZNode znode = ZNode.OfVaradhiTopic(varadhiTopicName);
        return this.zkMetaStore.zkPathExist(znode);
    }

    @Override
    public VaradhiTopic getVaradhiTopic(String varadhiTopicName) {
        ZNode znode = ZNode.OfVaradhiTopic(varadhiTopicName);
        return this.zkMetaStore.getZNodeDataAsPojo(znode, VaradhiTopic.class);
    }

    @Override
    public void deleteVaradhiTopic(String varadhiTopicName) {
        ZNode znode = ZNode.OfVaradhiTopic(varadhiTopicName);
        this.zkMetaStore.deleteZNode(znode);
    }

}
