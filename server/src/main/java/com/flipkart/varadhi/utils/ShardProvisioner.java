package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.spi.services.StorageSubscriptionService;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import lombok.extern.slf4j.Slf4j;

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
        provisionStorageSubscription(shard.getMainSubscription().getStorageSubscription(), project);

        RetrySubscription retrySub = shard.getRetrySubscription();
        for (int rqIndex = 0; rqIndex < retrySub.getMaxRetryCount(); rqIndex++) {
            int retryAttempt = rqIndex + 1;
            StorageSubscription<StorageTopic> retryStorageSub = retrySub.getStorageSubscriptionForRetry(retryAttempt);
            StorageTopic retryStorageTopic = retryStorageSub.getStorageTopic();
            provisionStorageTopic(retryStorageTopic, project);
            provisionStorageSubscription(retryStorageSub, project);
        }
        InternalCompositeSubscription dltSub = shard.getDeadLetterSubscription();
        StorageSubscription<StorageTopic> dltStorageSub = dltSub.getStorageSubscription();
        StorageTopic dltStorageTopic = dltStorageSub.getStorageTopic();
        provisionStorageTopic(dltStorageTopic, project);

        provisionStorageSubscription(dltStorageSub, project);
        log.info("Provisioned the Subscription: {}, Shard:{}", subscriptionName, shard.getShardId());
    }

    private void provisionStorageTopic(StorageTopic storageTopic, Project project) {
        if (storageTopicService.exists(storageTopic.getName())) {
            log.info("StorageTopic:{} already exists, re-using it.", storageTopic.getName());
        } else {
            storageTopicService.create(storageTopic, project);
            log.info("storageTopic:{} provisioned.", storageTopic.getName());
        }
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

    private void deProvisionShard(String subscriptionName, SubscriptionUnitShard shard, Project project) {
        InternalCompositeSubscription dltSub = shard.getDeadLetterSubscription();
        StorageSubscription<StorageTopic> dltStorageSub = dltSub.getStorageSubscription();
        StorageTopic dltStorageTopic = dltStorageSub.getTopicPartitions().getTopic();
        deProvisionStorageSubscription(dltStorageSub, project);
        deProvisionStorageTopic(dltStorageTopic, project);

        RetrySubscription retrySub = shard.getRetrySubscription();
        for (int rqIndex = 0; rqIndex < retrySub.getMaxRetryCount(); rqIndex++) {
            int rqAttempt = rqIndex + 1;
            StorageSubscription<StorageTopic> retryStorageSub = retrySub.getStorageSubscriptionForRetry(rqAttempt);
            StorageTopic retryStorageTopic = retryStorageSub.getTopicPartitions().getTopic();
            deProvisionStorageSubscription(retryStorageSub, project);
            deProvisionStorageTopic(retryStorageTopic, project);
        }
        deProvisionStorageSubscription(shard.getMainSubscription().getStorageSubscription(), project);
        log.info("deProvisioned the Subscription: {}, Shard:{}", subscriptionName, shard.getShardId());
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
