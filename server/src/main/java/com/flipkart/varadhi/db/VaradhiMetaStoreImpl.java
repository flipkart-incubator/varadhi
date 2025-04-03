package com.flipkart.varadhi.db;

import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.auth.IamPolicyRecord;
import com.flipkart.varadhi.entities.auth.ResourceType;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import com.flipkart.varadhi.spi.db.IamPolicy.IamPolicyMetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.org.OrgMetaStore;
import com.flipkart.varadhi.spi.db.project.ProjectMetaStore;
import com.flipkart.varadhi.spi.db.subscription.SubscriptionMetaStore;
import com.flipkart.varadhi.spi.db.team.TeamMetaStore;
import com.flipkart.varadhi.spi.db.topic.TopicMetaStore;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.flipkart.varadhi.db.ZNode.*;
import static com.flipkart.varadhi.entities.VersionedEntity.NAME_SEPARATOR;

@Slf4j
public class VaradhiMetaStoreImpl implements TopicMetaStore, SubscriptionMetaStore, TeamMetaStore, OrgMetaStore,
    IamPolicyMetaStore, ProjectMetaStore {

    private final ZKMetaStore zkMetaStore;

    /**
     * Constructs a new VaradhiMetaStore instance.
     *
     * <p>This constructor initializes the ZooKeeper-based metadata store and ensures
     * all required entity paths exist.
     *
     * @param zkMetaStore the ZooKeeper curator framework instance, must not be null
     * @throws IllegalArgumentException if zkCurator is null
     * @throws MetaStoreException       if initialization fails or required paths cannot be created
     */
    public VaradhiMetaStoreImpl(ZKMetaStore zkMetaStore) {
        this.zkMetaStore = zkMetaStore;
        ensureEntityTypePathExists();
    }

    /**
     * Ensures that all required entity type paths exist in ZooKeeper.
     * Creates missing paths if necessary.
     *
     * @throws MetaStoreException if path creation fails
     */
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
    public void createOrg(Org org) {
        ZNode znode = ZNode.ofOrg(org.getName());
        zkMetaStore.createZNodeWithData(znode, org);
    }

    /**
     * Retrieves an organization by its name.
     *
     * @param orgName the name of the organization
     * @return the organization entity
     * @throws ResourceNotFoundException if organization doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public Org getOrg(String orgName) {
        ZNode znode = ZNode.ofOrg(orgName);
        return zkMetaStore.getZNodeDataAsPojo(znode, Org.class);
    }

    /**
     * Retrieves all organizations.
     *
     * @return list of all organizations
     * @throws MetaStoreException if there's an error during retrieval
     */
    @Override
    public List<Org> getOrgs() {
        ZNode znode = ZNode.ofEntityType(ORG);
        return zkMetaStore.listChildren(znode).stream().map(this::getOrg).toList();
    }

    /**
     * Checks if an organization exists.
     *
     * @param orgName the name of the organization
     * @return true if organization exists, false otherwise
     * @throws MetaStoreException if there's an error checking existence
     */
    @Override
    public boolean checkOrgExists(String orgName) {
        ZNode znode = ZNode.ofOrg(orgName);
        return zkMetaStore.zkPathExist(znode);
    }

    /**
     * Deletes an organization by its name.
     *
     * @param orgName the name of the organization to delete
     * @throws ResourceNotFoundException            if organization doesn't exist
     * @throws InvalidOperationForResourceException if organization has associated teams
     * @throws MetaStoreException                   if there's an error during deletion
     */
    @Override
    public void deleteOrg(String orgName) {
        ZNode znode = ZNode.ofOrg(orgName);
        zkMetaStore.deleteZNode(znode);
    }

    /**
     * Retrieves a named filter by its name within a specified organization.
     *
     * @param orgName    the name of the organization
     * @return the named filter with the specified name
     */
    @Override
    public OrgFilters getOrgFilter(String orgName) {
        ZNode znode = ZNode.ofOrgNamedFilter(orgName);
        return zkMetaStore.getZNodeDataAsPojo(znode, OrgFilters.class);
    }

    /**
     * Updates a named filter within a specified organization.
     *
     * @param orgName    the name of the organization
     * @param filterName the name of the filter
     * @return the updated named filter
     */
    @Override
    public void updateOrgFilter(String orgName, String filterName, OrgFilters orgFilters) {
        ZNode znode = ZNode.ofOrgNamedFilter(orgName);
        zkMetaStore.updateZNodeWithData(znode, orgFilters);
    }

    /**
     * Creates a new named filter in the system.
     *
     * @param namedFilter the named filter to create
     * @throws IllegalArgumentException   if namedFilter is null or invalid
     * @throws DuplicateResourceException if named filter already exists
     * @throws MetaStoreException         if there's an error during creation
     */
    @Override
    public OrgFilters createOrgFilter(String orgName, OrgFilters namedFilter) {
        ZNode znode = ZNode.ofOrgNamedFilter(orgName);
        zkMetaStore.createZNodeWithData(znode, namedFilter);
        return namedFilter;
    }

    /**
     * @param orgName
     */
    @Override
    public void deleteOrgFilter(String orgName) {
        ZNode znode = ZNode.ofOrgNamedFilter(orgName);
        zkMetaStore.deleteZNode(znode);
    }

    /**
     * Creates a new project.
     *
     * @param project the project to create
     * @throws IllegalArgumentException   if project is null or invalid
     * @throws DuplicateResourceException if project already exists
     * @throws ResourceNotFoundException  if associated team or organization doesn't exist
     * @throws MetaStoreException         if there's an error during creation
     */
    @Override
    public void createProject(Project project) {
        ZNode znode = ZNode.ofProject(project.getName());
        zkMetaStore.createTrackedZNodeWithData(znode, project, ResourceType.PROJECT);
    }

    /**
     * Retrieves a project by its name.
     *
     * @param projectName the name of the project
     * @return the project entity
     * @throws ResourceNotFoundException if project doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public Project getProject(String projectName) {
        ZNode znode = ZNode.ofProject(projectName);
        return zkMetaStore.getZNodeDataAsPojo(znode, Project.class);
    }

    /**
     * Retrieves all projects for a team.
     *
     * @param teamName the name of the team
     * @param orgName  the name of the organization
     * @return list of projects
     * @throws ResourceNotFoundException if team or organization doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public List<Project> getProjects(String teamName, String orgName) {
        ZNode znode = ZNode.ofEntityType(PROJECT);
        return zkMetaStore.listChildren(znode)
                          .stream()
                          .map(this::getProject)
                          .filter(project -> matchesTeamAndOrg(project, teamName, orgName))
                          .toList();
    }

    /**
     * Checks if a project exists.
     *
     * @param projectName the name of the project
     * @return true if project exists, false otherwise
     * @throws MetaStoreException if there's an error checking existence
     */
    @Override
    public boolean checkProjectExists(String projectName) {
        ZNode znode = ZNode.ofProject(projectName);
        return zkMetaStore.zkPathExist(znode);
    }

    /**
     * Updates an existing project.
     *
     * @param project the project to update
     * @throws ResourceNotFoundException            if project doesn't exist
     * @throws IllegalArgumentException             if project update is invalid or no changes detected
     * @throws InvalidOperationForResourceException if there's a version conflict
     * @throws MetaStoreException                   if there's an error during update
     */
    @Override
    public void updateProject(Project project) {
        ZNode znode = ZNode.ofProject(project.getName());
        zkMetaStore.updateTrackedZNodeWithData(znode, project, ResourceType.PROJECT);
    }

    /**
     * Deletes a project.
     *
     * @param projectName the name of the project to delete
     * @throws ResourceNotFoundException            if project doesn't exist
     * @throws InvalidOperationForResourceException if project has associated topics or subscriptions
     * @throws MetaStoreException                   if there's an error during deletion
     */
    @Override
    public void deleteProject(String projectName) {
        ZNode znode = ZNode.ofProject(projectName);
        zkMetaStore.deleteTrackedZNode(znode, ResourceType.PROJECT);
    }

    /**
     * Checks if a project belongs to the specified team and organization.
     *
     * @param project  the project to check
     * @param teamName the team name to match
     * @param orgName  the organization name to match
     * @return true if the project belongs to the specified team and organization
     */
    private boolean matchesTeamAndOrg(Project project, String teamName, String orgName) {
        return project.getTeam().equals(teamName) && project.getOrg().equals(orgName);
    }

    /**
     * Creates a new subscription.
     *
     * @param subscription the subscription to create
     * @throws IllegalArgumentException   if subscription is null or invalid
     * @throws DuplicateResourceException if subscription already exists
     * @throws ResourceNotFoundException  if associated project or topic doesn't exist
     * @throws MetaStoreException         if there's an error during creation
     */
    @Override
    public void createSubscription(VaradhiSubscription subscription) {
        ZNode znode = ZNode.ofSubscription(subscription.getName());
        zkMetaStore.createTrackedZNodeWithData(znode, subscription, ResourceType.SUBSCRIPTION);
    }

    /**
     * Retrieves a subscription by its name.
     *
     * @param subscriptionName the name of the subscription
     * @return the subscription entity
     * @throws ResourceNotFoundException if subscription doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public VaradhiSubscription getSubscription(String subscriptionName) {
        ZNode znode = ZNode.ofSubscription(subscriptionName);
        return zkMetaStore.getZNodeDataAsPojo(znode, VaradhiSubscription.class);
    }

    /**
     * Retrieves all subscription names across all projects.
     *
     * @return list of all subscription names
     * @throws MetaStoreException if there's an error during retrieval
     */
    @Override
    public List<String> getAllSubscriptionNames() {
        ZNode znode = ZNode.ofEntityType(SUBSCRIPTION);
        return zkMetaStore.listChildren(znode).stream().toList();
    }

    /**
     * Retrieves subscription names for a project.
     *
     * @param projectName the name of the project
     * @return list of subscription names
     * @throws ResourceNotFoundException if project doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public List<String> getSubscriptionNames(String projectName) {
        String projectPrefix = projectName + NAME_SEPARATOR;
        ZNode znode = ZNode.ofEntityType(SUBSCRIPTION);
        return zkMetaStore.listChildren(znode).stream().filter(name -> name.contains(projectPrefix)).toList();
    }

    /**
     * Checks if a subscription exists.
     *
     * @param subscriptionName the name of the subscription
     * @return true if subscription exists, false otherwise
     * @throws MetaStoreException if there's an error checking existence
     */
    @Override
    public boolean checkSubscriptionExists(String subscriptionName) {
        ZNode znode = ZNode.ofSubscription(subscriptionName);
        return zkMetaStore.zkPathExist(znode);
    }

    /**
     * Updates an existing subscription.
     *
     * @param subscription the subscription to update
     * @throws ResourceNotFoundException            if subscription doesn't exist
     * @throws IllegalArgumentException             if subscription update is invalid
     * @throws InvalidOperationForResourceException if there's a version conflict
     * @throws MetaStoreException                   if there's an error during update
     */
    @Override
    public void updateSubscription(VaradhiSubscription subscription) {
        ZNode znode = ZNode.ofSubscription(subscription.getName());
        zkMetaStore.updateTrackedZNodeWithData(znode, subscription, ResourceType.SUBSCRIPTION);
    }

    /**
     * Deletes a subscription by its name.
     *
     * @param subscriptionName the name of the subscription to delete
     * @throws ResourceNotFoundException if subscription doesn't exist
     * @throws MetaStoreException        if there's an error during deletion
     */
    @Override
    public void deleteSubscription(String subscriptionName) {
        ZNode znode = ZNode.ofSubscription(subscriptionName);
        zkMetaStore.deleteTrackedZNode(znode, ResourceType.SUBSCRIPTION);
    }

    /**
     * Creates a new team.
     *
     * @param team the team to create
     * @throws IllegalArgumentException   if team is null or invalid
     * @throws DuplicateResourceException if team already exists
     * @throws ResourceNotFoundException  if associated organization doesn't exist
     * @throws MetaStoreException         if there's an error during creation
     */
    @Override
    public void createTeam(Team team) {
        ZNode znode = ZNode.ofTeam(team.getOrg(), team.getName());
        zkMetaStore.createZNodeWithData(znode, team);
    }

    /**
     * Retrieves a team by its name and organization.
     *
     * @param teamName the name of the team
     * @param orgName  the name of the organization
     * @return the team entity
     * @throws ResourceNotFoundException if team or organization doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public Team getTeam(String teamName, String orgName) {
        ZNode znode = ZNode.ofTeam(orgName, teamName);
        return zkMetaStore.getZNodeDataAsPojo(znode, Team.class);
    }

    /**
     * Retrieves all teams in an organization.
     *
     * @param orgName the organization name
     * @return list of teams
     * @throws ResourceNotFoundException if organization doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public List<Team> getTeams(String orgName) {
        return getTeamNames(orgName).stream().map(teamName -> getTeam(teamName, orgName)).toList();
    }

    /**
     * Retrieves team names for an organization.
     *
     * @param orgName the organization name
     * @return list of team names
     * @throws ResourceNotFoundException if organization doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public List<String> getTeamNames(String orgName) {
        String orgPrefix = orgName + RESOURCE_NAME_SEPARATOR;
        ZNode znode = ZNode.ofEntityType(TEAM);

        return zkMetaStore.listChildren(znode)
                          .stream()
                          .filter(teamName -> teamName.startsWith(orgPrefix))
                          .map(teamName -> teamName.split(RESOURCE_NAME_SEPARATOR)[1])
                          .toList();
    }

    /**
     * Checks if a team exists in an organization.
     *
     * @param teamName the name of the team
     * @param orgName  the name of the organization
     * @return true if team exists, false otherwise
     * @throws MetaStoreException if there's an error checking existence
     */
    @Override
    public boolean checkTeamExists(String teamName, String orgName) {
        ZNode znode = ZNode.ofTeam(orgName, teamName);
        return zkMetaStore.zkPathExist(znode);
    }

    /**
     * Deletes a team from an organization.
     *
     * @param teamName the name of the team
     * @param orgName  the name of the organization
     * @throws ResourceNotFoundException            if team or organization doesn't exist
     * @throws InvalidOperationForResourceException if team has associated projects
     * @throws MetaStoreException                   if there's an error during deletion
     */
    @Override
    public void deleteTeam(String teamName, String orgName) {
        ZNode znode = ZNode.ofTeam(orgName, teamName);
        zkMetaStore.deleteZNode(znode);
    }

    /**
     * Creates a new topic.
     *
     * @param topic the topic to create
     * @throws IllegalArgumentException   if topic is null or invalid
     * @throws DuplicateResourceException if topic already exists
     * @throws ResourceNotFoundException  if associated project doesn't exist
     * @throws MetaStoreException         if there's an error during creation
     */
    @Override
    public void createTopic(VaradhiTopic topic) {
        ZNode znode = ZNode.ofTopic(topic.getName());
        zkMetaStore.createTrackedZNodeWithData(znode, topic, ResourceType.TOPIC);
    }

    /**
     * Retrieves a topic by its name.
     *
     * @param topicName the name of the topic
     * @return the topic entity
     * @throws ResourceNotFoundException if topic doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public VaradhiTopic getTopic(String topicName) {
        ZNode znode = ZNode.ofTopic(topicName);
        return zkMetaStore.getZNodeDataAsPojo(znode, VaradhiTopic.class);
    }

    /**
     * Retrieves topic names for a project.
     *
     * @param projectName the name of the project
     * @return list of topic names
     * @throws ResourceNotFoundException if project doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public List<String> getTopicNames(String projectName) {
        String projectPrefix = projectName + NAME_SEPARATOR;
        ZNode znode = ZNode.ofEntityType(TOPIC);
        return zkMetaStore.listChildren(znode).stream().filter(name -> name.startsWith(projectPrefix)).toList();
    }

    /**
     * Checks if a topic exists.
     *
     * @param topicName the name of the topic
     * @return true if topic exists, false otherwise
     * @throws MetaStoreException if there's an error checking existence
     */
    @Override
    public boolean checkTopicExists(String topicName) {
        ZNode znode = ZNode.ofTopic(topicName);
        return zkMetaStore.zkPathExist(znode);
    }

    /**
     * Updates an existing topic.
     *
     * @param topic the topic to update
     * @throws ResourceNotFoundException            if topic doesn't exist
     * @throws IllegalArgumentException             if topic update is invalid
     * @throws InvalidOperationForResourceException if there's a version conflict
     * @throws MetaStoreException                   if there's an error during update
     */
    @Override
    public void updateTopic(VaradhiTopic topic) {
        ZNode znode = ZNode.ofTopic(topic.getName());
        zkMetaStore.updateTrackedZNodeWithData(znode, topic, ResourceType.TOPIC);
    }

    /**
     * Deletes a topic by its name.
     *
     * @param topicName the name of the topic to delete
     * @throws ResourceNotFoundException            if topic doesn't exist
     * @throws InvalidOperationForResourceException if topic has associated subscriptions
     * @throws MetaStoreException                   if there's an error during deletion
     */
    @Override
    public void deleteTopic(String topicName) {
        ZNode znode = ZNode.ofTopic(topicName);
        zkMetaStore.deleteTrackedZNode(znode, ResourceType.TOPIC);
    }

    /**
     * @param iamPolicyRecord
     */
    /**
     * Creates a new IAM policy record in the metadata store.
     *
     * @param iamPolicyRecord the IAM policy record to create
     * @throws IllegalArgumentException   if iamPolicyRecord is null or invalid
     * @throws DuplicateResourceException if a policy with the same name already exists
     * @throws MetaStoreException         if there's an error during creation
     */
    @Override
    public void createIamPolicyRecord(IamPolicyRecord iamPolicyRecord) {
        ZNode znode = ZNode.ofIamPolicy(iamPolicyRecord.getName());
        zkMetaStore.createZNodeWithData(znode, iamPolicyRecord);
    }

    /**
     * Retrieves an IAM policy record by its resource ID.
     *
     * @param authResourceId the unique identifier of the IAM policy
     * @return the IAM policy record
     * @throws IllegalArgumentException  if authResourceId is null or empty
     * @throws ResourceNotFoundException if the policy doesn't exist
     * @throws MetaStoreException        if there's an error during retrieval
     */
    @Override
    public IamPolicyRecord getIamPolicyRecord(String authResourceId) {
        ZNode znode = ZNode.ofIamPolicy(authResourceId);
        return zkMetaStore.getZNodeDataAsPojo(znode, IamPolicyRecord.class);
    }

    /**
     * Checks if an IAM policy record exists for the given resource ID.
     *
     * @param authResourceId the unique identifier of the IAM policy
     * @return true if the policy exists, false otherwise
     * @throws IllegalArgumentException if authResourceId is null or empty
     * @throws MetaStoreException       if there's an error checking existence
     */
    @Override
    public boolean isIamPolicyRecordPresent(String authResourceId) {
        ZNode znode = ZNode.ofIamPolicy(authResourceId);
        return zkMetaStore.zkPathExist(znode);
    }

    /**
     * Updates an existing IAM policy record.
     *
     * @param iamPolicyRecord the IAM policy record to update
     * @throws IllegalArgumentException             if iamPolicyRecord is null or invalid
     * @throws ResourceNotFoundException            if the policy doesn't exist
     * @throws InvalidOperationForResourceException if the update operation is invalid
     * @throws MetaStoreException                   if there's an error during update
     */
    @Override
    public void updateIamPolicyRecord(IamPolicyRecord iamPolicyRecord) {
        ZNode znode = ZNode.ofIamPolicy(iamPolicyRecord.getName());
        zkMetaStore.updateZNodeWithData(znode, iamPolicyRecord);
    }

    /**
     * Deletes an IAM policy record.
     *
     * @param authResourceId the unique identifier of the IAM policy to delete
     * @throws IllegalArgumentException  if authResourceId is null or empty
     * @throws ResourceNotFoundException if the policy doesn't exist
     * @throws MetaStoreException        if there's an error during deletion
     */
    @Override
    public void deleteIamPolicyRecord(String authResourceId) {
        ZNode znode = ZNode.ofIamPolicy(authResourceId);
        zkMetaStore.deleteZNode(znode);
    }
}
