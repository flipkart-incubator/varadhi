package com.flipkart.varadhi.spi.mock;

import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.JsonMapper;
import com.flipkart.varadhi.entities.Org;
import com.flipkart.varadhi.entities.OrgDetails;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.Region;
import com.flipkart.varadhi.entities.Team;
import com.flipkart.varadhi.entities.VaradhiSubscription;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.db.MetaStoreEventListener;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import com.flipkart.varadhi.spi.db.OrgStore;
import com.flipkart.varadhi.spi.db.ProjectStore;
import com.flipkart.varadhi.spi.db.RegionStore;
import com.flipkart.varadhi.spi.db.SubscriptionStore;
import com.flipkart.varadhi.spi.db.TeamStore;
import com.flipkart.varadhi.spi.db.TopicStore;

import lombok.SneakyThrows;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Simple in-memory MetaStore implementation for testing purposes.
 * Only supports basic operations needed for the producer benchmark.
 */
public class InMemoryMetaStore implements MetaStore {

    private final ConcurrentMap<String, byte[]> orgs = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, byte[]> orgFilters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, byte[]> teams = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, byte[]> projects = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, byte[]> topics = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, byte[]> subscriptions = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, byte[]> regions = new ConcurrentHashMap<>();

    @SneakyThrows
    private <T> byte[] toBytes(T object) {
        return JsonMapper.jsonSerializeAsBytes(object);
    }

    @SneakyThrows
    private <T> T fromBytes(byte[] bytes, Class<T> clazz) {
        return JsonMapper.jsonDeserialize(bytes, clazz);
    }

    @Override
    public OrgStore orgs() {
        return new InMemoryOrgStore();
    }

    @Override
    public TeamStore teams() {
        return new InMemoryTeamStore();
    }

    @Override
    public ProjectStore projects() {
        return new InMemoryProjectStore();
    }

    @Override
    public TopicStore topics() {
        return new InMemoryTopicStore();
    }

    @Override
    public SubscriptionStore subscriptions() {
        return new InMemorySubscriptionStore();
    }

    @Override
    public RegionStore regions() {
        return new InMemoryRegionStore();
    }

    @Override
    public boolean registerEventListener(MetaStoreEventListener listener) {
        return true; // No-op for testing
    }

    // Minimal store implementations for testing
    private class InMemoryOrgStore implements OrgStore {
        @Override
        public void create(Org org) {
            if (orgs.containsKey(org.getName())) {
                throw new MetaStoreException("Org already exists: " + org.getName());
            }
            orgs.put(org.getName(), toBytes(org));
        }

        @Override
        public Org get(String orgName) {
            var bytes = orgs.get(orgName);
            return bytes == null ? null : fromBytes(bytes, Org.class);
        }

        @Override
        public List<Org> getAll() {
            return orgs.values().stream().map(o -> fromBytes(o, Org.class)).toList();
        }

        @Override
        public boolean exists(String orgName) {
            return orgs.containsKey(orgName);
        }

        @Override
        public void delete(String orgName) {
            if (!orgs.containsKey(orgName)) {
                throw new MetaStoreException("Org does not exist: " + orgName);
            }
            orgs.remove(orgName);
        }

        @Override
        public OrgFilters getFilter(String orgName) {
            byte[] bytes = orgFilters.get(orgName);
            return bytes == null ? null : fromBytes(bytes, OrgFilters.class);
        }

        @Override
        public void updateFilter(String orgName, OrgFilters filters) {
            if (!orgFilters.containsKey(orgName)) {
                throw new MetaStoreException("OrgFilters do not exist for org: " + orgName);
            }
            orgFilters.put(orgName, toBytes(filters));
        }

        @Override
        public OrgFilters createFilter(String orgName, OrgFilters namedFilter) {
            if (orgFilters.containsKey(orgName)) {
                throw new MetaStoreException("OrgFilters already exist for org: " + orgName);
            }
            orgFilters.put(orgName, toBytes(namedFilter));
            return namedFilter;
        }

        @Override
        public List<OrgDetails> getAllOrgDetails() {
            return orgs.values()
                       .stream()
                       .map(o -> fromBytes(o, Org.class))
                       .map(org -> new OrgDetails(org, getFilter(org.getName())))
                       .toList();
        }
    }


    private class InMemoryTeamStore implements TeamStore {

        @Override
        public void create(Team team) {
            if (teams.containsKey(fqn(team))) {
                throw new MetaStoreException("Team already exists: " + team.getName());
            }
            teams.put(fqn(team), toBytes(team));
        }

        @Override
        public Team get(String teamName, String orgName) {
            byte[] bytes = teams.get(fqn(orgName, teamName));
            return bytes == null ? null : fromBytes(bytes, Team.class);
        }

        @Override
        public List<Team> getAll(String orgName) {
            return teams.entrySet()
                        .stream()
                        .filter(e -> e.getKey().startsWith(orgName + "$"))
                        .map(e -> fromBytes(e.getValue(), Team.class))
                        .toList();
        }

        @Override
        public List<String> getAllNames(String orgName) {
            return teams.entrySet().stream().filter(e -> e.getKey().startsWith(orgName + "$")).map(e -> {
                String key = e.getKey();
                return key.substring(key.indexOf('$') + 1);
            }).toList();
        }

        @Override
        public boolean exists(String teamName, String orgName) {
            return teams.containsKey(fqn(orgName, teamName));
        }

        @Override
        public void delete(String teamName, String orgName) {
            String key = fqn(orgName, teamName);
            if (!teams.containsKey(key)) {
                throw new MetaStoreException("Team does not exist: " + teamName + " in org: " + orgName);
            }
            teams.remove(key);
        }

    }


    private class InMemoryProjectStore implements ProjectStore {
        @Override
        public void create(Project project) {
            if (projects.containsKey(project.getName())) {
                throw new MetaStoreException("Project already exists: " + project.getName());
            }
            projects.put(project.getName(), toBytes(project));
        }

        @Override
        public void update(Project project) {
            if (!projects.containsKey(project.getName())) {
                throw new MetaStoreException("Project does not exist: " + project.getName());
            }
            projects.put(project.getName(), toBytes(project));
        }

        @Override
        public Project get(String projectName) {
            byte[] bytes = projects.get(projectName);
            return bytes == null ? null : fromBytes(bytes, Project.class);
        }

        @Override
        public List<Project> getAll() {
            return projects.values().stream().map(p -> fromBytes(p, Project.class)).toList();
        }

        @Override
        public List<Project> getAll(String orgName, String teamName) {
            return getAll().stream().filter(p -> orgName.equals(p.getOrg()) && teamName.equals(p.getTeam())).toList();
        }

        @Override
        public boolean exists(String projectName) {
            return projects.containsKey(projectName);
        }

        @Override
        public void delete(String projectName) {
            if (!projects.containsKey(projectName)) {
                throw new MetaStoreException("Project does not exist: " + projectName);
            }
            projects.remove(projectName);
        }
    }


    private class InMemoryRegionStore implements RegionStore {
        @Override
        public void create(Region region) {
            String key = region.getName();
            if (regions.containsKey(key)) {
                throw new MetaStoreException("Region already exists: " + key);
            }
            regions.put(key, toBytes(region));
        }

        @Override
        public void update(Region region) {
            String key = region.getName();
            if (!regions.containsKey(key)) {
                throw new ResourceNotFoundException("Region(" + key + ") not found.");
            }
            regions.put(key, toBytes(region));
        }

        @Override
        public Region get(String regionName) {
            byte[] bytes = regions.get(regionName);
            return bytes == null ? null : fromBytes(bytes, Region.class);
        }

        @Override
        public List<Region> getAll() {
            return regions.values().stream().map(r -> fromBytes(r, Region.class)).toList();
        }

        @Override
        public boolean exists(String regionName) {
            return regions.containsKey(regionName);
        }

        @Override
        public void delete(String regionName) {
            if (!regions.containsKey(regionName)) {
                throw new MetaStoreException("Region does not exist: " + regionName);
            }
            regions.remove(regionName);
        }
    }


    private class InMemoryTopicStore implements TopicStore {
        @Override
        public void create(VaradhiTopic topic) {
            if (topics.containsKey(topic.getName())) {
                throw new MetaStoreException("Topic already exists: " + topic.getName());
            }
            topics.put(topic.getName(), toBytes(topic));
        }

        @Override
        public void update(VaradhiTopic topic) {
            if (!topics.containsKey(topic.getName())) {
                throw new MetaStoreException("Topic does not exist: " + topic.getName());
            }
            topics.put(topic.getName(), toBytes(topic));
        }

        @Override
        public VaradhiTopic get(String topicName) {
            byte[] bytes = topics.get(topicName);
            return bytes == null ? null : fromBytes(bytes, VaradhiTopic.class);
        }

        @Override
        public List<VaradhiTopic> getAll() {
            return topics.values().stream().map(t -> fromBytes(t, VaradhiTopic.class)).toList();
        }

        @Override
        public List<String> getAllNames(String projectName) {
            return getAll().stream()
                           .filter(t -> projectName.equals(t.getProjectName()))
                           .map(VaradhiTopic::getName)
                           .toList();
        }

        @Override
        public boolean exists(String topicName) {
            return topics.containsKey(topicName);
        }

        @Override
        public void delete(String topicName) {
            if (!topics.containsKey(topicName)) {
                throw new MetaStoreException("Topic does not exist: " + topicName);
            }
            topics.remove(topicName);
        }
    }


    private class InMemorySubscriptionStore implements SubscriptionStore {

        @Override
        public void create(VaradhiSubscription subscription) {
            if (subscriptions.containsKey(fqn(subscription))) {
                throw new MetaStoreException("Subscription already exists: " + subscription.getName());
            }
            subscriptions.put(fqn(subscription), toBytes(subscription));
        }

        @Override
        public void update(VaradhiSubscription subscription) {
            if (!subscriptions.containsKey(fqn(subscription))) {
                throw new MetaStoreException("Subscription does not exist: " + subscription.getName());
            }
            subscriptions.put(fqn(subscription), toBytes(subscription));
        }

        @Override
        public VaradhiSubscription get(String subscriptionFQN) {
            byte[] bytes = subscriptions.get(subscriptionFQN);
            return bytes == null ? null : fromBytes(bytes, VaradhiSubscription.class);
        }

        @Override
        public List<String> getAllNames() {
            return getAll().stream().map(InMemoryMetaStore::fqn).toList();
        }

        @Override
        public List<String> getAllNames(String projectName) {
            return getAll().stream()
                           .filter(s -> projectName.equals(s.getProject()))
                           .map(InMemoryMetaStore::fqn)
                           .toList();
        }

        @Override
        public boolean exists(String subscriptionFQN) {
            return subscriptions.containsKey(subscriptionFQN);
        }

        @Override
        public void delete(String subscriptionFQN) {
            if (!subscriptions.containsKey(subscriptionFQN)) {
                throw new MetaStoreException("Subscription does not exist: " + subscriptionFQN);
            }
            subscriptions.remove(subscriptionFQN);
        }

        private List<VaradhiSubscription> getAll() {
            return subscriptions.values().stream().map(s -> fromBytes(s, VaradhiSubscription.class)).toList();
        }
    }

    static String fqn(String... parts) {
        return String.join("$", parts);
    }

    static String fqn(VaradhiSubscription subscription) {
        return fqn(subscription.getTopic(), subscription.getName());
    }

    static String fqn(Team team) {
        return fqn(team.getOrg(), team.getName());
    }
}
