package com.flipkart.varadhi.db;

import com.flipkart.varadhi.exceptions.MetaStoreException;
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


    @BeforeAll
    public static void setUp() throws Exception {
        zkCuratorTestingServer = new TestingServer();
        CuratorFramework zkCuratorFramework = CuratorFrameworkFactory.newClient(
                zkCuratorTestingServer.getConnectString(), new ExponentialBackoffRetry(1000, 1));
        zkCuratorFramework.start();
        zkMetaStore = new ZKMetaStore(zkCuratorFramework);
    }

    @AfterAll
    public static void closeTest() throws IOException {
        zkCuratorTestingServer.close();
    }

    @Test
    public void testZKData() {

        String response = zkMetaStore.create("sample-testing-data", 1, "/sample-testing-node");
        Assertions.assertEquals(response, "/sample-testing-node");

        String data = zkMetaStore.get("/sample-testing-node", String.class);
        Assertions.assertEquals(data, "sample-testing-data");

        zkMetaStore.create("", 1, "/sample-testing-node/1");
        zkMetaStore.create("", 1, "/sample-testing-node/2");
        List<String> zkChildrenNodes = zkMetaStore.list("/sample-testing-node");
        Assertions.assertEquals(2, zkChildrenNodes.size());


        zkMetaStore.delete("/sample-testing-node/1");
        zkMetaStore.delete("/sample-testing-node/2");
        zkMetaStore.delete("/sample-testing-node");
        MetaStoreException exception = Assertions.assertThrows(
                MetaStoreException.class,
                () -> zkMetaStore.get("/sample-testing-node", String.class)
        );
        Assertions.assertEquals("Failed to get entity /sample-testing-node ", exception.getMessage());
    }

}
