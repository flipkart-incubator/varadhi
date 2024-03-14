package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.exceptions.NotImplementedException;
import com.flipkart.varadhi.pulsar.ClientProvider;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.services.MessagingException;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;

import java.util.HashSet;
import java.util.Set;

import static com.flipkart.varadhi.pulsar.util.EntityHelper.getNamespace;

@Slf4j
public class PulsarTopicService implements StorageTopicService<PulsarStorageTopic> {
    private ClientProvider clientProvider;

    public PulsarTopicService(ClientProvider clientProvider) {
        this.clientProvider = clientProvider;
    }

    public void create(PulsarStorageTopic topic, Project project) {
        try {
            createTenant(project.getOrg());
            createNamespace(project.getOrg(), project.getName());
            //TODO:: Check any other attributes to set on the topic e.g. retention, secure etc.
            clientProvider.getAdminClient().topics().createPartitionedTopic(topic.getName(), topic.getPartitionCount());
            log.info("Created the pulsar topic:{}", topic.getName());
        } catch (PulsarAdminException e) {
            if (e instanceof PulsarAdminException.ConflictException) {
                // No specific handling as of now.
                log.error("Topic {} already exists.", topic.getName());
            }
            throw new MessagingException(e);
        }
    }

    @Override
    public PulsarStorageTopic get(String topicName) {
        throw new NotImplementedException();
    }

    @Override
    public void delete(String topicName) {
        try {
            clientProvider.getAdminClient().topics().deletePartitionedTopic(topicName, false, false);
            log.info("Deleted the pulsar topic:{}", topicName);
        } catch (PulsarAdminException e) {
            throw new MessagingException(e);
        }
    }

    @Override
    public boolean exists(String topicName) {
        try {
            return clientProvider.getAdminClient().topics().getPartitionedTopicMetadata((topicName)) != null;
        } catch (PulsarAdminException.NotFoundException e) {
            //exception occurs if Topic is not found
            return false;
        } catch (PulsarAdminException e) {
            throw new MessagingException(
                    String.format("Failed to check existence of Topic. Error: %s.", e.getMessage()), e);
        }
    }

    private void createTenant(String tenantName) throws PulsarAdminException {
        if (!clientProvider.getAdminClient().tenants().getTenants().contains(tenantName)) {
            Clusters clusters = clientProvider.getAdminClient().clusters();
            Set<String> clusterSet = (clusters == null) ? new HashSet<>() : new HashSet<>(clusters.getClusters());
            TenantInfo tenantInfo = TenantInfoImpl.builder().allowedClusters(clusterSet).build();
            clientProvider.getAdminClient().tenants().createTenant(tenantName, tenantInfo);
            log.info("Created the tenant:{}", tenantName);
        }
    }

    private void createNamespace(String tenantName, String projectName) throws PulsarAdminException {
        String namespace = getNamespace(tenantName, projectName);
        if (!clientProvider.getAdminClient().namespaces().getNamespaces(tenantName).contains(namespace)) {
            clientProvider.getAdminClient().namespaces().createNamespace(namespace);
            log.info("Created the namespace:{}, in tenant:{}", namespace, tenantName);
        }
    }
}
