package com.flipkart.varadhi.pulsar.clients;

import com.flipkart.varadhi.exceptions.VaradhiException;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class AdminClientTest {
    private PulsarAdmin pulsarAdmin;
    private Topics topics;
    private AdminClient adminClient;

    @BeforeEach
    public void setUp() {
        pulsarAdmin = mock(PulsarAdmin.class);
        topics = mock(Topics.class);
        doReturn(topics).when(pulsarAdmin).topics();
        adminClient = spy(new AdminClient(pulsarAdmin));
    }

    @Test
    public void testCreate() throws PulsarAdminException {
        Map<String, String> properties = new HashMap<>();
        PulsarStorageTopic topic = new PulsarStorageTopic("testTopic", 1);
        doNothing().when(topics).createPartitionedTopic(anyString(), anyInt(), anyMap());
        adminClient.create(topic);
        verify(topics, times(1)).createPartitionedTopic(eq(topic.getName()), eq(1), eq(properties));
    }

    @Test
    public void testCreate_PulsarAdminException() throws PulsarAdminException {
        PulsarStorageTopic topic = new PulsarStorageTopic("testTopic", 1);
        doThrow(PulsarAdminException.class).when(topics).createPartitionedTopic(anyString(), anyInt(), anyMap());
        assertThrows(VaradhiException.class, () -> adminClient.create(topic));
        verify(pulsarAdmin.topics(), times(1)).createPartitionedTopic(anyString(), anyInt(), anyMap());
    }
}
