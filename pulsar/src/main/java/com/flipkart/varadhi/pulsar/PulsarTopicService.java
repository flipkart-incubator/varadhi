package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.entities.InternalQueueCategory;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.entities.TopicCapacityPolicy;
import com.flipkart.varadhi.entities.TopicPartitions;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.pulsar.util.TopicPlanner;
import com.flipkart.varadhi.spi.services.MessagingException;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.Clusters;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.flipkart.varadhi.pulsar.util.EntityHelper.getNamespace;

@Slf4j
public class PulsarTopicService implements StorageTopicService<PulsarStorageTopic> {
    private final ClientProvider clientProvider;
    private final TopicPlanner topicPlanner;

    public PulsarTopicService(ClientProvider clientProvider, TopicPlanner topicPlanner) {
        this.clientProvider = clientProvider;
        this.topicPlanner = topicPlanner;
    }

    public void create(PulsarStorageTopic topic, Project project) {
        createTenant(project.getOrg());
        createNamespace(project.getOrg(), project.getName());
        try {
            PartitionedTopicMetadata topicMeta = clientProvider.getAdminClient()
                                                               .topics()
                                                               .getPartitionedTopicMetadata((topic.getName()));
            if (topicMeta.partitions != topic.getPartitionCount()) {
                // This can be enhanced to validate other properties of the topic as well to establish
                // pre-existing Pulsar topic is valid to be associated with requested PulsarStorageTopic
                log.error(
                    "Topic {} properties mismatch: PartitionCount expected:{} found:{}",
                    topic.getName(),
                    topic.getPartitionCount(),
                    topicMeta.partitions
                );
                throw new MessagingException(
                    String.format(
                        "Found existing pulsar topic %s with different config, can't re-use it.",
                        topic.getName()
                    )
                );
            }
            log.info("Found well formed pulsar topic {}, re-using it.", topic.getName());
            return;
        } catch (PulsarAdminException e) {
            if (!(e instanceof PulsarAdminException.NotFoundException)) {
                throw new MessagingException(e);
            }
        }
        try {

            //TODO:: Check any other attributes to set on the topic e.g. retention, secure etc.
            clientProvider.getAdminClient().topics().createPartitionedTopic(topic.getName(), topic.getPartitionCount());
            log.info("Created the pulsar topic: {} with partitions: {}", topic.getName(), topic.getPartitionCount());
        } catch (PulsarAdminException e) {
            throw new MessagingException(e);
        }
    }

    @Override
    public List<TopicPartitions<PulsarStorageTopic>> shardTopic(
        PulsarStorageTopic topic,
        TopicCapacityPolicy capacity,
        InternalQueueCategory category
    ) {
        List<TopicPartitions<PulsarStorageTopic>> topicPartitions = new ArrayList<>();
        int shardCount = topicPlanner.getShardCount(topic, capacity, category);
        int partitionsPerShard = topic.getPartitionCount() / shardCount;
        for (int shardId = 0; shardId < shardCount; shardId++) {
            topicPartitions.add(
                TopicPartitions.byPartitions(topic, getPartitionsForShard(shardId, partitionsPerShard))
            );
        }
        return topicPartitions;
    }

    private int[] getPartitionsForShard(int shardId, int partitionsPerShard) {
        int[] partitions = new int[partitionsPerShard];
        for (int partId = 0; partId < partitionsPerShard; partId++) {
            // Calculate the partition ID based on the shard ID and the partition ID within the shard
            partitions[partId] = shardId * partitionsPerShard + partId;
        }
        return partitions;
    }

    @Override
    public void delete(String topicName, Project project) {
        try {
            clientProvider.getAdminClient().topics().deletePartitionedTopic(topicName, false, false);
            log.debug("Deleted the pulsar topic:{}", topicName);
        } catch (PulsarAdminException e) {
            if (e instanceof PulsarAdminException.NotFoundException) {
                log.warn("Pulsar topic {} not found, skipping delete.", topicName);
                return;
            }
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
                String.format("Failed to check existence of Topic. Error: %s.", e.getMessage()),
                e
            );
        }
    }

    private void createTenant(String tenantName) {
        try {
            if (!clientProvider.getAdminClient().tenants().getTenants().contains(tenantName)) {
                Clusters clusters = clientProvider.getAdminClient().clusters();
                Set<String> clusterSet = (clusters == null) ? new HashSet<>() : new HashSet<>(clusters.getClusters());
                TenantInfo tenantInfo = TenantInfoImpl.builder().allowedClusters(clusterSet).build();
                clientProvider.getAdminClient().tenants().createTenant(tenantName, tenantInfo);
                log.info("Created the tenant:{}", tenantName);
            }
        } catch (PulsarAdminException e) {
            throw new MessagingException(e);
        }
    }

    private void createNamespace(String tenantName, String projectName) {
        try {
            String namespace = getNamespace(tenantName, projectName);
            if (!clientProvider.getAdminClient().namespaces().getNamespaces(tenantName).contains(namespace)) {
                clientProvider.getAdminClient().namespaces().createNamespace(namespace);
                log.info("Created the namespace:{}, in tenant:{}", namespace, tenantName);
            }
        } catch (PulsarAdminException e) {
            throw new MessagingException(e);
        }
    }
}
