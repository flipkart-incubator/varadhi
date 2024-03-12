package com.flipkart.varadhi.core;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.VaradhiException;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.services.StorageTopicFactory;
import com.flipkart.varadhi.spi.services.StorageTopicService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static com.flipkart.varadhi.entities.VersionedEntity.INITIAL_VERSION;
import static org.mockito.Mockito.*;

public class VaradhiTopicServiceTest {

    StorageTopicFactory<StorageTopic> storageTopicFactory;
    private VaradhiTopicFactory varadhiTopicFactory;
    private StorageTopicService<StorageTopic> storageTopicService;
    private MetaStore metaStore;
    private VaradhiTopicService varadhiTopicService;
    private Project project;
    private final String region = "local";
    private final String topicName = "testTopic";
    private String vTopicName;
    private CapacityPolicy capacityPolicy;


    @BeforeEach
    public void setUp() {
        storageTopicService = mock(StorageTopicService.class);
        metaStore = mock(MetaStore.class);
        storageTopicFactory = mock(StorageTopicFactory.class);
        varadhiTopicFactory = spy(new VaradhiTopicFactory(storageTopicFactory, region));
        varadhiTopicService = new VaradhiTopicService(storageTopicService, metaStore);
        project = new Project("default", INITIAL_VERSION, "", "public", "public");
        vTopicName = String.format("%s.%s", project.getName(), topicName);
        String pTopicName =
                String.format("persistent://%s/%s/%s", project.getOrg(), project.getName(), vTopicName);
        capacityPolicy = CapacityPolicy.getDefault();
        PulsarStorageTopic pTopic = PulsarStorageTopic.from(pTopicName, capacityPolicy);
        Mockito.doReturn(pTopic).when(storageTopicFactory).getTopic(vTopicName, project, capacityPolicy);
    }

    @Test
    public void createVaradhiTopic() {
        TopicResource topicResource = getTopicResource(topicName, project);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        varadhiTopicService.create(varadhiTopic, project);
        verify(metaStore, times(1)).createTopic(varadhiTopic);
        StorageTopic st = varadhiTopic.getProduceTopicForRegion(region).getStorageTopic();
        verify(storageTopicService, times(1)).create(st, project);
        verify(storageTopicFactory, times(1)).getTopic(vTopicName, project, capacityPolicy);
    }

    @Test
    public void createVaradhiTopicWhenMetaStoreFails() {
        TopicResource topicResource = getTopicResource(topicName, project);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        StorageTopic st = varadhiTopic.getProduceTopicForRegion(region).getStorageTopic();
        doThrow(new VaradhiException("Some error")).when(metaStore).createTopic(varadhiTopic);
        Exception exception =
                Assertions.assertThrows(VaradhiException.class, () -> varadhiTopicService.create(
                        varadhiTopic,
                        project
                ));
        verify(metaStore, times(1)).createTopic(varadhiTopic);
        verify(storageTopicService, times(1)).create(st, project);
        Assertions.assertEquals(exception.getClass(), VaradhiException.class);
    }

    @Test
    public void createVaradhiTopicWhenStorageTopicServiceFails() {
        TopicResource topicResource = getTopicResource(topicName, project);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        StorageTopic st = varadhiTopic.getProduceTopicForRegion(region).getStorageTopic();
        doThrow(new VaradhiException("Some error")).when(storageTopicService).create(st, project);
        Exception exception =
                Assertions.assertThrows(VaradhiException.class, () -> varadhiTopicService.create(
                        varadhiTopic,
                        project
                ));
        verify(metaStore, times(0)).createTopic(varadhiTopic);
        verify(storageTopicService, times(1)).create(st, project);
        Assertions.assertEquals(exception.getClass(), VaradhiException.class);
    }

    @Test
    public void deleteVaradhiTopicSuccessfully() {
        TopicResource topicResource = getTopicResource(topicName, project);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        StorageTopic st = varadhiTopic.getProduceTopicForRegion(region).getStorageTopic();
        when(storageTopicService.exists(st.getName())).thenReturn(true);
        when(metaStore.getTopic(varadhiTopic.getName())).thenReturn(varadhiTopic);

        varadhiTopicService.delete(varadhiTopic.getName());

        verify(storageTopicService, times(1)).delete(st.getName());
        verify(metaStore, times(1)).deleteTopic(varadhiTopic.getName());
    }

    @Test
    public void deleteVaradhiTopicWhenStorageTopicDoesNotExist() {
        TopicResource topicResource = getTopicResource(topicName, project);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        StorageTopic st = varadhiTopic.getProduceTopicForRegion(region).getStorageTopic();
        when(storageTopicService.exists(st.getName())).thenReturn(false);
        when(metaStore.getTopic(varadhiTopic.getName())).thenReturn(varadhiTopic);

        varadhiTopicService.delete(varadhiTopic.getName());

        verify(storageTopicService, times(0)).delete(st.getName());
        verify(metaStore, times(1)).deleteTopic(varadhiTopic.getName());
    }

    @Test
    public void deleteVaradhiTopicWhenMetaStoreFails() {
        TopicResource topicResource = getTopicResource(topicName, project);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        StorageTopic st = varadhiTopic.getProduceTopicForRegion(region).getStorageTopic();
        when(storageTopicService.exists(st.getName())).thenReturn(true);
        when(metaStore.getTopic(varadhiTopic.getName())).thenReturn(varadhiTopic);
        doThrow(new VaradhiException("Some error")).when(metaStore).deleteTopic(varadhiTopic.getName());

        Exception exception = Assertions.assertThrows(
                VaradhiException.class,
                () -> varadhiTopicService.delete(varadhiTopic.getName())
        );

        verify(storageTopicService, times(1)).delete(st.getName());
        verify(metaStore, times(1)).deleteTopic(varadhiTopic.getName());
        Assertions.assertEquals(exception.getClass(), VaradhiException.class);
    }

    @Test
    public void checkVaradhiTopicExistsWhenTopicExists() {
        TopicResource topicResource = getTopicResource(topicName, project);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        when(metaStore.checkTopicExists(varadhiTopic.getName())).thenReturn(true);

        boolean exists = varadhiTopicService.exists(varadhiTopic.getName());

        Assertions.assertTrue(exists);
        verify(metaStore, times(1)).checkTopicExists(varadhiTopic.getName());
    }

    @Test
    public void checkVaradhiTopicExistsWhenTopicDoesNotExist() {
        TopicResource topicResource = getTopicResource(topicName, project);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(project, topicResource);
        when(metaStore.checkTopicExists(varadhiTopic.getName())).thenReturn(false);

        boolean exists = varadhiTopicService.exists(varadhiTopic.getName());

        Assertions.assertFalse(exists);
        verify(metaStore, times(1)).checkTopicExists(varadhiTopic.getName());
    }

    private TopicResource getTopicResource(String topicName, Project project) {
        return new TopicResource(
                topicName,
                1,
                project.getName(),
                true,
                capacityPolicy
        );
    }
}

