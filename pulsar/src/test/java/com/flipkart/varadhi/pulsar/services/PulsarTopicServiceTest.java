package com.flipkart.varadhi.pulsar.services;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.common.Constants;
import com.flipkart.varadhi.entities.Project;
import com.flipkart.varadhi.pulsar.ClientProvider;
import com.flipkart.varadhi.pulsar.PulsarTopicService;
import com.flipkart.varadhi.pulsar.config.PulsarConfig;
import com.flipkart.varadhi.pulsar.entities.PulsarOffset;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.pulsar.util.EntityHelper;
import com.flipkart.varadhi.pulsar.util.TopicPlanner;
import com.flipkart.varadhi.spi.services.MessagingException;
import com.flipkart.varadhi.entities.JsonMapper;
import org.apache.pulsar.client.admin.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class PulsarTopicServiceTest {
    private static final String TEST_TOPIC = "testTopic";
    private PulsarAdmin pulsarAdmin;
    private Topics topics;
    private Tenants tenants;
    private Namespaces namespaces;
    private PulsarTopicService pulsarTopicService;
    private ClientProvider clientProvider;
    private Project project;

    @BeforeEach
    public void setUp() {
        pulsarAdmin = mock(PulsarAdmin.class);
        topics = mock(Topics.class);
        tenants = mock(Tenants.class);
        namespaces = mock(Namespaces.class);
        project = Project.of("testNamespace", "", "public", "testTenant");
        doReturn(topics).when(pulsarAdmin).topics();
        doReturn(tenants).when(pulsarAdmin).tenants();
        doReturn(namespaces).when(pulsarAdmin).namespaces();
        clientProvider = mock(ClientProvider.class);
        doReturn(pulsarAdmin).when(clientProvider).getAdminClient();
        PulsarConfig pulsarConfig = new PulsarConfig();
        pulsarTopicService = new PulsarTopicService(clientProvider, new TopicPlanner(pulsarConfig));

        JsonMapper.getMapper().registerSubtypes(new NamedType(PulsarOffset.class, "PulsarOffset"));
    }

    @Test
    public void testCreate() throws PulsarAdminException {
        PulsarStorageTopic topic = PulsarStorageTopic.of(TEST_TOPIC, 1, Constants.DEFAULT_TOPIC_CAPACITY);
        doThrow(new PulsarAdminException.NotFoundException(new RuntimeException(""), "topic not found", 409)).when(
            topics
        ).getPartitionedTopicMetadata(topic.getName());
        doNothing().when(topics).createPartitionedTopic(anyString(), eq(1));
        pulsarTopicService.create(topic, project);
        verify(topics, times(1)).createPartitionedTopic(eq(topic.getName()), eq(1));
    }

    @Test
    public void testCreate_PulsarAdminException() throws PulsarAdminException {
        PulsarStorageTopic topic = PulsarStorageTopic.of(TEST_TOPIC, 1, Constants.DEFAULT_TOPIC_CAPACITY);
        doThrow(new PulsarAdminException.NotFoundException(new RuntimeException(""), "topic not found", 409)).when(
            topics
        ).getPartitionedTopicMetadata(topic.getName());
        doThrow(PulsarAdminException.class).when(topics).createPartitionedTopic(anyString(), eq(1));
        assertThrows(MessagingException.class, () -> pulsarTopicService.create(topic, project));
        verify(pulsarAdmin.topics(), times(1)).createPartitionedTopic(anyString(), eq(1));
    }

    @Test
    public void testCreate_ConflictException() throws PulsarAdminException {
        PulsarStorageTopic topic = PulsarStorageTopic.of(TEST_TOPIC, 1, Constants.DEFAULT_TOPIC_CAPACITY);
        doThrow(new PulsarAdminException.NotFoundException(new RuntimeException(""), "topic not found", 409)).when(
            topics
        ).getPartitionedTopicMetadata(topic.getName());
        doThrow(PulsarAdminException.class).when(topics).createPartitionedTopic(anyString(), eq(1));
        doThrow(new PulsarAdminException.ConflictException(new RuntimeException(""), "duplicate topic error", 409))
                                                                                                                   .when(
                                                                                                                       topics
                                                                                                                   )
                                                                                                                   .createPartitionedTopic(
                                                                                                                       anyString(),
                                                                                                                       eq(
                                                                                                                           1
                                                                                                                       )
                                                                                                                   );
        MessagingException me = Assertions.assertThrows(
            MessagingException.class,
            () -> pulsarTopicService.create(topic, project)
        );
        Assertions.assertInstanceOf(PulsarAdminException.ConflictException.class, me.getCause());
        Assertions.assertEquals("duplicate topic error", me.getMessage());
        verify(pulsarAdmin.topics(), times(1)).createPartitionedTopic(anyString(), eq(1));

    }

    @Test
    public void testCreate_NewTenantNamespace() throws PulsarAdminException {
        String newTenant = "testTenantNew";
        Project projectNew = Project.of("projectNew", "", "public", newTenant);
        String newNamespace = EntityHelper.getNamespace(newTenant, projectNew.getName());
        PulsarStorageTopic topic = PulsarStorageTopic.of(TEST_TOPIC, 1, Constants.DEFAULT_TOPIC_CAPACITY);
        doThrow(new PulsarAdminException.NotFoundException(new RuntimeException(""), "topic not found", 409)).when(
            topics
        ).getPartitionedTopicMetadata(topic.getName());
        doNothing().when(topics).createPartitionedTopic(anyString(), eq(1));
        pulsarTopicService.create(topic, projectNew);
        verify(topics, times(1)).createPartitionedTopic(eq(topic.getName()), eq(1));
        verify(tenants, times(1)).createTenant(eq(projectNew.getOrg()), any());
        verify(namespaces, times(1)).createNamespace(eq(newNamespace));
    }
}
