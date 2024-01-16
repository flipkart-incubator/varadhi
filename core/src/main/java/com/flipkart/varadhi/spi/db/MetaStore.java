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

    List<String> getTopicNames(String projectName);

    void createTopic(VaradhiTopic varadhiTopicName);

    VaradhiTopic getTopic(String varadhiTopicName);

    boolean checkTopicExists(String varadhiTopicName);

    void deleteTopic(String varadhiTopicName);

    List<String> getSubscriptionNames(String projectName);

    void createSubscription(VaradhiSubscription subscription);

    VaradhiSubscription getSubscription(String subscriptionName);

    int updateSubscription(VaradhiSubscription subscription);

    boolean checkSubscriptionExists(String subscriptionName);

    void deleteSubscription(String subscriptionName);
}
