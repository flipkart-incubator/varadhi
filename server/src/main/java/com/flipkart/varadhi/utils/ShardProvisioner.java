package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.spi.services.StorageSubscriptionService;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class ShardProvisioner {
    StorageTopicService<StorageTopic> storageTopicService;
    StorageSubscriptionService<StorageSubscription<StorageTopic>> storageSubscriptionService;

    public ShardProvisioner(
            StorageSubscriptionService<StorageSubscription<StorageTopic>> storageSubscriptionService,
            StorageTopicService<StorageTopic> storageTopicService
    ) {
        this.storageSubscriptionService = storageSubscriptionService;
        this.storageTopicService = storageTopicService;
    }

    public void provision(VaradhiSubscription varadhiSub, Project project) {
        for (int i = 0; i < varadhiSub.getShards().getShardCount(); i++) {
            SubscriptionUnitShard shard = varadhiSub.getShards().getShard(i);
            provisionShard(varadhiSub.getName(), shard, project);
        }
        log.info("Provisioned the Subscription: {}", varadhiSub.getName());
    }

    public void deProvision(VaradhiSubscription varadhiSub, Project project) {
        for (int i = 0; i < varadhiSub.getShards().getShardCount(); i++) {
            SubscriptionUnitShard shard = varadhiSub.getShards().getShard(i);
            deProvisionShard(varadhiSub.getName(), shard, project);
        }
        log.info("deProvisioned the Subscription: {}", varadhiSub.getName());
    }
    private void provisionShard(String subscriptionName, SubscriptionUnitShard shard, Project project) {
        // provision main sub for shard.
        provisionCompositeSubscription(shard.getMainSubscription(), project, false);
        RetrySubscription retrySub = shard.getRetrySubscription();
        for (int rqIndex = 0; rqIndex < retrySub.getMaxRetryCount(); rqIndex++) {
            provisionCompositeSubscription(retrySub.getSubscriptionForRetry(rqIndex + 1), project, true);
        }
        provisionCompositeSubscription(shard.getDeadLetterSubscription(), project, true);
        log.info("Provisioned the Subscription: {}, Shard:{}", subscriptionName, shard.getShardId());
    }

    private void provisionCompositeSubscription(
            InternalCompositeSubscription compositeSubscription, Project project, boolean provisionTopics
    ) {
        List<StorageSubscription<StorageTopic>> storageSubs = compositeSubscription.getActiveSubscriptions();
        storageSubs.forEach(storageSub -> {
            if (provisionTopics) {
                provisionStorageTopic(storageSub.getStorageTopic(), project);
            }
            provisionStorageSubscription(storageSub, project);
        });
    }

    private void provisionStorageSubscription(StorageSubscription<StorageTopic> storageSub, Project project) {
        if (storageSubscriptionService.exists(
                storageSub.getName(), storageSub.getTopicPartitions().getTopic().getName())) {
            log.info("StorageSubscription:{} already exists, re-using it.", storageSub.getName());
        } else {
            storageSubscriptionService.create(storageSub, project);
            log.info("StorageSubscription:{} provisioned.", storageSub.getName());
        }
    }

    private void provisionStorageTopic(StorageTopic storageTopic, Project project) {
        if (storageTopicService.exists(storageTopic.getName())) {
            log.info("StorageTopic:{} already exists, re-using it.", storageTopic.getName());
        } else {
            storageTopicService.create(storageTopic, project);
            log.info("storageTopic:{} provisioned.", storageTopic.getName());
        }
    }

    private void deProvisionShard(String subscriptionName, SubscriptionUnitShard shard, Project project) {
        deProvisionCompositeSubscription(shard.getDeadLetterSubscription(), project, true);
        RetrySubscription retrySub = shard.getRetrySubscription();
        for (int rqIndex = 0; rqIndex < retrySub.getMaxRetryCount(); rqIndex++) {
            int rqAttempt = rqIndex + 1;
            deProvisionCompositeSubscription(retrySub.getSubscriptionForRetry(rqAttempt), project, true);
        }
        deProvisionCompositeSubscription(shard.getMainSubscription(), project, false);
        log.info("deProvisioned the Subscription: {}, Shard:{}", subscriptionName, shard.getShardId());
    }

    private void deProvisionCompositeSubscription(
            InternalCompositeSubscription compositeSubscription, Project project, boolean deProvisionTopics
    ) {
        List<StorageSubscription<StorageTopic>> storageSubs = compositeSubscription.getActiveSubscriptions();
        storageSubs.forEach(storageSub -> {
            deProvisionStorageSubscription(storageSub, project);
            if (deProvisionTopics) {
                deProvisionStorageTopic(storageSub.getStorageTopic(), project);
            }
        });
    }

    private void deProvisionStorageTopic(StorageTopic storageTopic, Project project) {
        if (!storageTopicService.exists(storageTopic.getName())) {
            log.info("StorageTopic:{} not found, skipping delete.", storageTopic.getName());
        } else {
            storageTopicService.delete(storageTopic.getName(), project);
            log.info("storageTopic:{} deleted.", storageTopic.getName());
        }
    }

    private void deProvisionStorageSubscription(StorageSubscription<StorageTopic> storageSub, Project project) {
        if (!storageSubscriptionService.exists(
                storageSub.getName(), storageSub.getTopicPartitions().getTopic().getName())) {
            log.info("StorageSubscription:{} not found, skipping delete.", storageSub.getName());
        } else {
            storageSubscriptionService.delete(storageSub, project);
            log.info("StorageSubscription:{} deleted.", storageSub.getName());
        }
    }

}
