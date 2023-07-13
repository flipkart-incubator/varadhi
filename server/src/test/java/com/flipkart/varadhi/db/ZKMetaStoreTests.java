package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.TopicResource;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.imps.GetDataBuilderImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;

public class ZKMetaStoreTests {

    @Mock
    private CuratorFramework curatorFramework;

    ZKMetaStore metaStore;


    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        metaStore = new ZKMetaStore(curatorFramework);
    }

    @Test
    public void testGetZKData() {

        String stringfiedResource =
                "{\"name\":\"sampleTopic\",\"version\":0,\"project\":\"sampleProject\",\"grouped\":true,\"exclusiveSubscription\":true}";
        GetDataBuilder dataBuilder = new GetDataBuilderImpl(null, null, null, null, false) {
            @Override
            public byte[] forPath(String path) {
                return stringfiedResource.getBytes();
            }
        };

        when(curatorFramework.getData()).thenReturn(dataBuilder);
        TopicResource result = metaStore.get("resourcePath", TopicResource.class);
        Assertions.assertEquals(result.getName(), "sampleTopic");
    }

}
