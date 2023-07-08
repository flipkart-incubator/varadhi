package com.flipkart.varadhi.services;

import com.flipkart.varadhi.db.MetaStore;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.VaradhiException;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

public class VaradhiTopicServiceTest {

    StorageTopicFactory<StorageTopic> storageTopicFactory;
    private VaradhiTopicFactory varadhiTopicFactory;
    private StorageTopicService<StorageTopic> storageTopicService;
    private MetaStore metaStore;
    private VaradhiTopicService varadhiTopicService;

    @BeforeEach
    public void setUp() {
        storageTopicService = mock(StorageTopicService.class);
        metaStore = mock(MetaStore.class);
        storageTopicFactory = mock(StorageTopicFactory.class);
        varadhiTopicFactory = spy(new VaradhiTopicFactory(storageTopicFactory));
        varadhiTopicService = new VaradhiTopicService(storageTopicService, metaStore);
        //TODO::check it, not a circular dependency though
        PulsarStorageTopic pTopic = new PulsarStorageTopic("public.default.testTopic.Main.local", 1);
        doReturn(pTopic).when(storageTopicFactory).getTopic("public.default.testTopic.Main.local", null);
    }

    @Test
    public void createVaradhiTopic() {
        TopicResource topicResource = new TopicResource("testTopic", 1, "testProject", true, false, null);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(topicResource);
        varadhiTopicService.create(varadhiTopic);
        verify(metaStore, times(1)).createVaradhiTopic(varadhiTopic);
        StorageTopic st = varadhiTopic.getInternalTopics().get(InternalTopic.TopicKind.Main).getStorageTopic();
        verify(storageTopicService, times(1)).create(st);
        verify(storageTopicFactory, times(1)).getTopic(st.getName(), null);
    }

    @Test
    public void createVaradhiTopicWhenMetaStoreFails() {
        TopicResource topicResource = new TopicResource("testTopic", 1, "testProject", true, false, null);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(topicResource);
        StorageTopic st = varadhiTopic.getInternalTopics().get(InternalTopic.TopicKind.Main).getStorageTopic();
        doThrow(new VaradhiException("Some error")).when(metaStore).createVaradhiTopic(varadhiTopic);
        Exception exception =
                Assertions.assertThrows(VaradhiException.class, () -> varadhiTopicService.create(varadhiTopic));
        verify(metaStore, times(1)).createVaradhiTopic(varadhiTopic);
        verify(storageTopicService, never()).create(st);
        Assertions.assertEquals(exception.getClass(), VaradhiException.class);
    }

    @Test
    public void createVaradhiTopicWhenStorageTopicServiceFails() {
        TopicResource topicResource = new TopicResource("testTopic", 1, "testProject", true, false, null);
        VaradhiTopic varadhiTopic = varadhiTopicFactory.get(topicResource);
        StorageTopic st = varadhiTopic.getInternalTopics().get(InternalTopic.TopicKind.Main).getStorageTopic();
        doThrow(new VaradhiException("Some error")).when(storageTopicService).create(st);
        Exception exception =
                Assertions.assertThrows(VaradhiException.class, () -> varadhiTopicService.create(varadhiTopic));
        verify(metaStore, times(1)).createVaradhiTopic(varadhiTopic);
        verify(storageTopicService, times(1)).create(st);
        Assertions.assertEquals(exception.getClass(), VaradhiException.class);
    }
}

