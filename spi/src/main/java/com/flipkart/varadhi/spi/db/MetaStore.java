package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;

import java.util.List;

/**
 * Interface defining operations for managing metadata storage in the Varadhi system.
 * Provides CRUD operations for organizations, teams, projects, topics, and subscriptions.
 */
public interface MetaStore {

    void createOrg(Org org);

    Org getOrg(String orgName);

    List<Org> getOrgs();

    boolean checkOrgExists(String orgName);

    void deleteOrg(String orgName);

    void createTeam(Team team);

    Team getTeam(String teamName, String orgName);

    List<Team> getTeams(String orgName);

    List<String> getTeamNames(String orgName);

    boolean checkTeamExists(String teamName, String orgName);

    void deleteTeam(String teamName, String orgName);

    void createProject(Project project);

    Project getProject(String projectName);

    List<Project> getProjects(String teamName, String orgName);

    List<Project> getAllProjects();

    boolean checkProjectExists(String projectName);

    void updateProject(Project project);

    void deleteProject(String project);

    void createTopic(VaradhiTopic topic);

    VaradhiTopic getTopic(String topicName);

    List<String> getTopicNames(String projectName);

    List<VaradhiTopic> getAllTopics();

    boolean checkTopicExists(String topicName);

    void updateTopic(VaradhiTopic topic);

    void deleteTopic(String topicName);

    void createSubscription(VaradhiSubscription subscription);

    VaradhiSubscription getSubscription(String subscriptionName);

    List<String> getAllSubscriptionNames();

    List<String> getSubscriptionNames(String projectName);

    boolean checkSubscriptionExists(String subscriptionName);

    void updateSubscription(VaradhiSubscription subscription);

    void deleteSubscription(String subscriptionName);

    boolean registerEventListener(MetaStoreEventListener listener);
}
