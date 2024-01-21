package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.*;

import java.util.List;

public interface MetaStore {
    void createOrg(Org org);

    Org getOrg(String orgName);

    boolean checkOrgExists(String orgName);

    void deleteOrg(String orgName);

    List<Org> getOrgs();

    List<String> getTeamNames(String orgName);

    List<Team> getTeams(String orgName);

    void createTeam(Team team);

    Team getTeam(String teamName, String orgName);

    boolean checkTeamExists(String teamName, String orgName);

    void deleteTeam(String teamName, String orgName);

    List<Project> getProjects(String teamName, String orgName);

    void createProject(Project project);

    Project getProject(String projectName);

    boolean checkProjectExists(String projectName);

    void deleteProject(String project);

    int updateProject(Project project);

    List<String> getVaradhiTopicNames(String projectName);

    void createVaradhiTopic(VaradhiTopic varadhiTopicName);

    VaradhiTopic getVaradhiTopic(String varadhiTopicName);

    boolean checkVaradhiTopicExists(String varadhiTopicName);

    void deleteVaradhiTopic(String varadhiTopicName);
}
