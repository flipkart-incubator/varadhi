package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.pulsar.entities.PulsarSubscription;
import com.flipkart.varadhi.spi.services.MessagingException;
import com.flipkart.varadhi.spi.services.StorageSubscriptionService;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;

@Slf4j
public class PulsarSubscriptionService implements StorageSubscriptionService<PulsarSubscription> {
    private final ClientProvider clientProvider;

    public PulsarSubscriptionService(ClientProvider clientProvider) {
        this.clientProvider = clientProvider;
    }

    @Override
    public void create(PulsarSubscription subscription, Project project) {
        try {
            PulsarAdmin admin = clientProvider.getAdminClient();
            //TODO::check configurability of messageId.latest.
            //TODO::Evaluate moving to REST API instead of java client, as no support to modify the
            // properties of subscription e.g. SubscriptionType, Description etc.
            String topicName = subscription.getTopicPartitions().getTopic().getName();
            admin.topics().createSubscription(topicName, subscription.getName(), MessageId.latest);
        } catch (PulsarAdminException e) {
            throw new MessagingException(e);
        }
    }

    @Override
    public void delete(PulsarSubscription subscription, Project project) {
        try {
            String topicName = subscription.getTopicPartitions().getTopic().getName();
            clientProvider.getAdminClient().topics().deleteSubscription(topicName, subscription.getName(), false);
        } catch (PulsarAdminException e) {
            throw new MessagingException(e);
        }
    }

    @Override
    public boolean exists(String subName, String topicName) {
        try {
            return clientProvider.getAdminClient().topics().getSubscriptions(topicName).contains(subName);
        } catch (PulsarAdminException e) {
            throw new MessagingException(e);
        }
    }
}
