package com.flipkart.varadhi.pulsar.services;

import com.flipkart.varadhi.entities.CapacityPolicy;
import com.flipkart.varadhi.exceptions.MessagingException;
import com.flipkart.varadhi.pulsar.clients.ClientProvider;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.Namespaces;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class PulsarTopicServiceTest {
    private PulsarAdmin pulsarAdmin;
    private Topics topics;
    private PulsarTopicService pulsarTopicService;
    private ClientProvider clientProvider;

    private static final String TENANT = "testTenant";
    private static final String NAMESPACE = "testNamespace";

    @BeforeEach
    public void setUp() {
        pulsarAdmin = mock(PulsarAdmin.class);
        topics = mock(Topics.class);
        Tenants tenants = mock(Tenants.class);
        Namespaces namespaces = mock(Namespaces.class);
        doReturn(topics).when(pulsarAdmin).topics();
        doReturn(tenants).when(pulsarAdmin).tenants();
        doReturn(namespaces).when(pulsarAdmin).namespaces();
        clientProvider = mock(ClientProvider.class);
        doReturn(pulsarAdmin).when(clientProvider).getAdminClient();
        pulsarTopicService = new PulsarTopicService(clientProvider);
    }

    @Test
    public void testCreate() throws PulsarAdminException {
        PulsarStorageTopic topic = PulsarStorageTopic.from("testTopic", CapacityPolicy.getDefault());
        doNothing().when(topics).createPartitionedTopic(anyString(), eq(1));
        pulsarTopicService.create(topic, TENANT, NAMESPACE);
        verify(topics, times(1)).createPartitionedTopic(eq(topic.getName()), eq(1));
    }

    @Test
    public void testCreate_PulsarAdminException() throws PulsarAdminException {
        PulsarStorageTopic topic = PulsarStorageTopic.from("testTopic", CapacityPolicy.getDefault());
        doThrow(PulsarAdminException.class).when(topics).createPartitionedTopic(anyString(), eq(1));
        assertThrows(MessagingException.class, () -> pulsarTopicService.create(topic, TENANT, NAMESPACE));
        verify(pulsarAdmin.topics(), times(1)).createPartitionedTopic(anyString(), eq(1));
    }

    @Test
    public void testCreate_ConflictException() throws PulsarAdminException {
        PulsarStorageTopic topic = PulsarStorageTopic.from("testTopic", CapacityPolicy.getDefault());
        doThrow(PulsarAdminException.class).when(topics).createPartitionedTopic(anyString(), eq(1));
        doThrow(new PulsarAdminException.ConflictException(
                new RuntimeException(""), "duplicate topic error", 409)).when(topics)
                .createPartitionedTopic(anyString(), eq(1));
        MessagingException me =
                Assertions.assertThrows(MessagingException.class, () -> pulsarTopicService.create(topic, TENANT, NAMESPACE));
        Assertions.assertTrue(me.getCause() instanceof PulsarAdminException.ConflictException);
        Assertions.assertEquals("duplicate topic error", me.getMessage());
        verify(pulsarAdmin.topics(), times(1)).createPartitionedTopic(anyString(), eq(1));

    }
}
