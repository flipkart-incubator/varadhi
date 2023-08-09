package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.VaradhiResource;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

public class ZKMetaStoreTests {

    private static TestingServer zkCuratorTestingServer;

    private static ZKMetaStore zkMetaStore;
    private static ZNodeKind testKind;

    static CuratorFramework zkCuratorFramework;

    @Setter
    @Getter
    @EqualsAndHashCode(callSuper = true)
    public static class TestData extends VaradhiResource {
        String data;

        public TestData(String name, int version, String data) {
            super(name, version);
            this.data = data;
        }
    }


    @BeforeAll
    public static void setUp() throws Exception {
        zkCuratorTestingServer = new TestingServer();
        zkCuratorFramework = CuratorFrameworkFactory.newClient(
                zkCuratorTestingServer.getConnectString(), new ExponentialBackoffRetry(1000, 1));
        zkCuratorFramework.start();
        testKind = new ZNodeKind("test");
        zkMetaStore = new ZKMetaStore(zkCuratorFramework);
        zkMetaStore.createZNode(ZNode.OfEntityType(testKind));
    }

    @AfterAll
    public static void closeTest() throws IOException {
        zkMetaStore.deleteZNode(ZNode.OfEntityType(testKind));
        zkCuratorTestingServer.close();
    }

    @Test
    public void testZKData() {
        TestData data1 = new TestData("test-node1", 0, "sample-testing-node1");
        TestData data2 = new TestData("test-node2", 0, "sample-testing-node2");
        zkMetaStore.createZNodeWithData(ZNode.OfKind(testKind, data1.getName()), data1);

        TestData g_data1 = zkMetaStore.getZNodeDataAsPojo(ZNode.OfKind(testKind, data1.getName()), TestData.class);
        Assertions.assertEquals(data1, g_data1);

        zkMetaStore.createZNodeWithData(ZNode.OfKind(testKind, data2.getName()), data2);
        List<String> zkChildrenNodes = zkMetaStore.listChildren(ZNode.OfEntityType(testKind));
        Assertions.assertEquals(2, zkChildrenNodes.size());
        
        zkMetaStore.deleteZNode(ZNode.OfKind(testKind, data1.getName()));
        zkMetaStore.deleteZNode(ZNode.OfKind(testKind, data2.getName()));

        Assertions.assertThrows(
                ResourceNotFoundException.class,
                () -> zkMetaStore.getZNodeDataAsPojo(ZNode.OfKind(testKind, data1.getName()), TestData.class),
                String.format("TestData(%s) not found.", data1.getName())
        );
    }

}
