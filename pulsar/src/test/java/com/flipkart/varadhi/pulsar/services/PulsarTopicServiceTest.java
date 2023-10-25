package com.flipkart.varadhi.pulsar.services;

import com.flipkart.varadhi.entities.CapacityPolicy;
import com.flipkart.varadhi.exceptions.MessagingException;
import com.flipkart.varadhi.pulsar.clients.ClientProvider;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class PulsarTopicServiceTest {
    private PulsarAdmin pulsarAdmin;
    private Topics topics;
    private PulsarTopicService pulsarTopicService;
    private ClientProvider clientProvider;

    @BeforeEach
    public void setUp() {
        pulsarAdmin = mock(PulsarAdmin.class);
        topics = mock(Topics.class);
        doReturn(topics).when(pulsarAdmin).topics();
        clientProvider = mock(ClientProvider.class);
        doReturn(pulsarAdmin).when(clientProvider).getAdminClient();
        pulsarTopicService = new PulsarTopicService(clientProvider);
    }

    @Test
    public void testCreate() throws PulsarAdminException {
        PulsarStorageTopic topic = PulsarStorageTopic.from("testTopic", CapacityPolicy.getDefault());
        doNothing().when(topics).createPartitionedTopic(anyString(), eq(1));
        pulsarTopicService.create(topic);
        verify(topics, times(1)).createPartitionedTopic(eq(topic.getName()), eq(1));
    }

    @Test
    public void testCreate_PulsarAdminException() throws PulsarAdminException {
        PulsarStorageTopic topic = PulsarStorageTopic.from("testTopic", CapacityPolicy.getDefault());
        doThrow(PulsarAdminException.class).when(topics).createPartitionedTopic(anyString(), eq(1));
        assertThrows(MessagingException.class, () -> pulsarTopicService.create(topic));
        verify(pulsarAdmin.topics(), times(1)).createPartitionedTopic(anyString(), eq(1));
    }
}
