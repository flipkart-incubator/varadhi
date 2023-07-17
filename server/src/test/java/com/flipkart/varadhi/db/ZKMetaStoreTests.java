package com.flipkart.varadhi.db;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class ZKMetaStoreTests {

    private static TestingServer zkCuratorTestingServer;
    private static CuratorFramework zkCuratorFramework;

    private static ZKMetaStore zkMetaStore;


    @BeforeAll
    public static void setUp() throws Exception {
        zkCuratorTestingServer = new TestingServer();
        zkCuratorFramework =
                CuratorFrameworkFactory.newClient(
                        zkCuratorTestingServer.getConnectString(), new ExponentialBackoffRetry(0, 1));
        zkCuratorFramework.start();
        zkMetaStore = new ZKMetaStore(zkCuratorFramework);
    }

    @AfterAll
    public static void closeTest() throws IOException {
        zkCuratorTestingServer.close();
    }


    @Test
    public void testGetZKData() {
        zkMetaStore.create("sample-testing-data", 1, "/sample-testing-node");
        String data = zkMetaStore.get("/sample-testing-node", String.class);
        Assertions.assertEquals(data, "sample-testing-data");
    }

}
