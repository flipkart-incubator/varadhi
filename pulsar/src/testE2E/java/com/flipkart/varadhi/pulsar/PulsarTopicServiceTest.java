package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.entities.CapacityHelper;
import com.flipkart.varadhi.exceptions.MessagingException;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.pulsar.services.PulsarTopicService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;


public class PulsarTopicServiceTest extends PulsarTestBase {
    PulsarTopicService topicService;

    @BeforeAll
    public static void getPulsarConfig() {
        loadPulsarConfig();
    }

    @BeforeEach
    public void init() throws PulsarAdminException {
        super.init();
        topicService = new PulsarTopicService(clientProvider);
    }

    @Test
    public void testCreateTopic() throws PulsarAdminException {
        String topicFQDN = getRandomTopicFQDN();
        PulsarStorageTopic pt = PulsarStorageTopic.from(topicFQDN, CapacityHelper.getDefault());
        topicService.create(pt);
        validateTopicExists(topicFQDN);
    }

    @Test
    public void testCreateTopic_Duplicate() throws PulsarAdminException {
        String topicFQDN = getRandomTopicFQDN();
        PulsarStorageTopic pt = PulsarStorageTopic.from(topicFQDN, CapacityHelper.getDefault());
        topicService.create(pt);
        MessagingException m = Assertions.assertThrows(MessagingException.class, () -> topicService.create(pt));
        Throwable realFailure = m.getCause();
        Assertions.assertTrue(
                realFailure instanceof PulsarAdminException.ConflictException, "Duplicate Topic creation didn't fail.");
    }

    private void validateTopicExists(String topicFQDN) throws PulsarAdminException {
        List<String> topics = clientProvider.getAdminClient().topics().getPartitionedTopicList(getNamespace());
        Assertions.assertTrue(topics.contains(topicFQDN), String.format("Failed to find the topic %s.", topicFQDN));
    }
}
