package com.flipkart.varadhi.pulsar.services;

import com.flipkart.varadhi.entities.CapacityPolicy;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.exceptions.MessagingException;
import com.flipkart.varadhi.pulsar.clients.ClientProvider;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.pulsar.util.EntityHelper;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.Namespaces;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.flipkart.varadhi.Constants.INITIAL_VERSION;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class PulsarTopicServiceTest {
    private PulsarAdmin pulsarAdmin;
    private Topics topics;
    private Tenants tenants;
    private Namespaces namespaces;
    private PulsarTopicService pulsarTopicService;
    private ClientProvider clientProvider;
    private Project project;
    private static final String TEST_TOPIC = "testTopic";

    @BeforeEach
    public void setUp() {
        pulsarAdmin = mock(PulsarAdmin.class);
        topics = mock(Topics.class);
        tenants = mock(Tenants.class);
        namespaces = mock(Namespaces.class);
        project = new Project("testNamespace", INITIAL_VERSION, "", "public", "testTenant");
        doReturn(topics).when(pulsarAdmin).topics();
        doReturn(tenants).when(pulsarAdmin).tenants();
        doReturn(namespaces).when(pulsarAdmin).namespaces();
        clientProvider = mock(ClientProvider.class);
        doReturn(pulsarAdmin).when(clientProvider).getAdminClient();
        pulsarTopicService = new PulsarTopicService(clientProvider);
    }

    @Test
    public void testCreate() throws PulsarAdminException {
        PulsarStorageTopic topic = PulsarStorageTopic.from(TEST_TOPIC, CapacityPolicy.getDefault());
        doNothing().when(topics).createPartitionedTopic(anyString(), eq(1));
        pulsarTopicService.create(topic, project);
        verify(topics, times(1)).createPartitionedTopic(eq(topic.getName()), eq(1));
    }

    @Test
    public void testCreate_PulsarAdminException() throws PulsarAdminException {
        PulsarStorageTopic topic = PulsarStorageTopic.from(TEST_TOPIC, CapacityPolicy.getDefault());
        doThrow(PulsarAdminException.class).when(topics).createPartitionedTopic(anyString(), eq(1));
        assertThrows(MessagingException.class, () -> pulsarTopicService.create(topic, project));
        verify(pulsarAdmin.topics(), times(1)).createPartitionedTopic(anyString(), eq(1));
    }

    @Test
    public void testCreate_ConflictException() throws PulsarAdminException {
        PulsarStorageTopic topic = PulsarStorageTopic.from(TEST_TOPIC, CapacityPolicy.getDefault());
        doThrow(PulsarAdminException.class).when(topics).createPartitionedTopic(anyString(), eq(1));
        doThrow(new PulsarAdminException.ConflictException(
                new RuntimeException(""), "duplicate topic error", 409)).when(topics)
                .createPartitionedTopic(anyString(), eq(1));
        MessagingException me =
                Assertions.assertThrows(MessagingException.class, () -> pulsarTopicService.create(topic, project));
        Assertions.assertTrue(me.getCause() instanceof PulsarAdminException.ConflictException);
        Assertions.assertEquals("duplicate topic error", me.getMessage());
        verify(pulsarAdmin.topics(), times(1)).createPartitionedTopic(anyString(), eq(1));

    }

    @Test
    public void testCreate_NewTenantNamespace() throws PulsarAdminException {
        String newTenant = "testTenantNew";
        Project projectNew = new Project("projectNew", INITIAL_VERSION, "", "public", newTenant);
        String newNamespace = EntityHelper.getNamespace(newTenant, projectNew.getName());

        PulsarStorageTopic topic = PulsarStorageTopic.from(TEST_TOPIC, CapacityPolicy.getDefault());
        doNothing().when(topics).createPartitionedTopic(anyString(), eq(1));
        pulsarTopicService.create(topic, projectNew);
        verify(topics, times(1)).createPartitionedTopic(eq(topic.getName()), eq(1));
        verify(tenants, times(1)).createTenant(eq(projectNew.getOrg()), any());
        verify(namespaces, times(1)).createNamespace(eq(newNamespace));
    }
}
