package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.pulsar.util.EntityHelper;
import com.flipkart.varadhi.pulsar.util.TopicPlanner;
import com.flipkart.varadhi.spi.services.MessagingException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.flipkart.varadhi.entities.VersionedEntity.INITIAL_VERSION;


public class PulsarTopicServiceTest extends PulsarTestBase {
    PulsarTopicService topicService;
    Project project;

    @BeforeAll
    public static void getPulsarConfig() {
        loadPulsarConfig();
    }

    @BeforeEach
    public void init() throws PulsarAdminException {
        super.init();
        topicService = new PulsarTopicService(clientProvider, new TopicPlanner(pulsarConfig));
        project = new Project(NAMESPACE, INITIAL_VERSION, "", "public", TENANT);
    }

    @Test
    public void testCreateTopic() throws PulsarAdminException {
        String topicFQDN = getRandomTopicFQDN();
        PulsarStorageTopic pt = PulsarStorageTopic.from(topicFQDN, 1, TopicCapacityPolicy.getDefault());
        topicService.create(pt, project);
        validateTopicExists(topicFQDN);
    }

    @Test
    public void testDuplicateTopicWithSameConfigAllowed() {
        String topicFQDN = getRandomTopicFQDN();
        PulsarStorageTopic pt = PulsarStorageTopic.from(topicFQDN, 1, TopicCapacityPolicy.getDefault());
        topicService.create(pt, project);
        topicService.create(pt, project);
    }

    @Test
    public void testDuplicateTopicWithDifferentConfigNotAllowed() {
        String topicFQDN = getRandomTopicFQDN();
        PulsarStorageTopic pt1 = PulsarStorageTopic.from(topicFQDN, 2, TopicCapacityPolicy.getDefault());
        PulsarStorageTopic pt2 = PulsarStorageTopic.from(topicFQDN, 1, TopicCapacityPolicy.getDefault());
        topicService.create(pt1, project);
        MessagingException m =
                Assertions.assertThrows(MessagingException.class, () -> topicService.create(pt2, project));
        Assertions.assertEquals("Found existing pulsar topic %s with different config, can't re-use it.".formatted(pt1.getName()), m.getMessage());
    }

    @Test
    public void testCreate_NewTenantNamespace() throws PulsarAdminException {
        String newTenant = "testTenantNew";
        Project projectNew = new Project("projectNew", INITIAL_VERSION, "", "public", newTenant);
        String newNamespace = EntityHelper.getNamespace(newTenant, projectNew.getName());
        String topicFQDN = getRandomTopicFQDN();

        PulsarStorageTopic pt = PulsarStorageTopic.from(topicFQDN, 1, TopicCapacityPolicy.getDefault());
        topicService.create(pt, projectNew);

        validateTopicExists(topicFQDN);
        validateTenantExists(newTenant);
        validateNamespaceExists(newTenant, newNamespace);
    }

    private void validateTopicExists(String topicFQDN) throws PulsarAdminException {
        List<String> topics = clientProvider.getAdminClient().topics().getPartitionedTopicList(getNamespace());
        Assertions.assertTrue(topics.contains(topicFQDN), String.format("Failed to find the topic %s.", topicFQDN));
    }

    private void validateTenantExists(String tenant) throws PulsarAdminException {
        List<String> tenants = clientProvider.getAdminClient().tenants().getTenants();
        Assertions.assertTrue(tenants.contains(tenant), String.format("Failed to find the tenant %s.", tenant));
    }

    private void validateNamespaceExists(String tenant, String namespace) throws PulsarAdminException {
        List<String> namespaces = clientProvider.getAdminClient().namespaces().getNamespaces(tenant);
        Assertions.assertTrue(
                namespaces.contains(namespace), String.format("Failed to find the namespace %s.", namespace));
    }
}
