package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.MetaStoreEntity;
import com.flipkart.varadhi.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.exceptions.InvalidOperationForResourceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.spi.db.MetaStoreException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class ZKMetaStoreTests {

    CuratorFramework zkCuratorFramework;
    private TestingServer zkCuratorTestingServer;
    private ZKMetaStore zkMetaStore;
    private ZNodeKind testKind;
    TestData data1;
    ZNode zn;

    @BeforeEach
    public void setUp() throws Exception {
        zkCuratorTestingServer = new TestingServer();
        zkCuratorFramework = spy(CuratorFrameworkFactory.newClient(
                zkCuratorTestingServer.getConnectString(), new ExponentialBackoffRetry(1000, 1)));
        zkCuratorFramework.start();
        testKind = new ZNodeKind("test");
        zkMetaStore = new ZKMetaStore(zkCuratorFramework);
        zkMetaStore.createZNode(ZNode.OfEntityType(testKind));
        data1 = new TestData("test-node1", 0, "sample-testing-node1");
        zn = getZnode(data1.getName());
    }

    @AfterEach
    public void closeTest() throws IOException {
        zkMetaStore.deleteZNode(ZNode.OfEntityType(testKind));
        zkCuratorTestingServer.close();
    }


    @Test
    public void testZKData() {
        TestData data2 = new TestData("test-node2", 0, "sample-testing-node2");
        zkMetaStore.createZNodeWithData(ZNode.OfKind(testKind, data1.getName()), data1);

        TestData g_data1 = zkMetaStore.getZNodeDataAsPojo(ZNode.OfKind(testKind, data1.getName()), TestData.class);
        Assertions.assertEquals(data1, g_data1);

        zkMetaStore.createZNodeWithData(ZNode.OfKind(testKind, data2.getName()), data2);
        List<String> zkChildrenNodes = zkMetaStore.listChildren(ZNode.OfEntityType(testKind));
        Assertions.assertEquals(2, zkChildrenNodes.size());

        zkMetaStore.deleteZNode(ZNode.OfKind(testKind, data1.getName()));
        zkMetaStore.deleteZNode(ZNode.OfKind(testKind, data2.getName()));

        ResourceNotFoundException e = Assertions.assertThrows(
                ResourceNotFoundException.class,
                () -> zkMetaStore.getZNodeDataAsPojo(ZNode.OfKind(testKind, data1.getName()), TestData.class)
        );
        Assertions.assertEquals(String.format("test(%s) not found.", data1.getName()), e.getMessage());
    }

    private ZNode getZnode(String name) {
        return ZNode.OfKind(testKind, name);
    }

    @Test
    public void testCreateZNodeFailure() throws Exception {
        CreateBuilder builder = spy(zkCuratorFramework.create());
        doReturn(builder).when(zkCuratorFramework).create();
        doThrow(new KeeperException.BadVersionException()).when(builder).forPath(any());
        MetaStoreException e = Assertions.assertThrows(MetaStoreException.class, () -> zkMetaStore.createZNode(zn));
        Assertions.assertEquals(String.format("Failed to create path %s.", zn.getPath()), e.getMessage());
    }

    @Test
    public void testCreateWhenAlreadyExists() throws Exception {
        zkMetaStore.createZNode(zn);
        CreateBuilder builder = spy(zkCuratorFramework.create());
        doReturn(builder).when(zkCuratorFramework).create();
        zkMetaStore.createZNode(zn);
        verify(builder, never()).forPath(zn.getPath());
        zkMetaStore.deleteZNode(zn);
    }

    @Test
    public void testCreateZNodeWithDataFailure() throws Exception {
        CreateBuilder builder = spy(zkCuratorFramework.create());
        doReturn(builder).when(zkCuratorFramework).create();

        doThrow(new KeeperException.NodeExistsException()).when(builder).forPath(any(), any());
        validateException(
                DuplicateResourceException.class, String.format("%s(%s) already exists.", zn.getKind(), zn.getName()),
                () -> zkMetaStore.createZNodeWithData(zn, data1)
        );

        doThrow(new KeeperException.BadArgumentsException()).when(builder).forPath(any(), any());
        validateException(
                MetaStoreException.class, String.format("Failed to create %s(%s) at %s.", zn.getKind(), zn.getName(),
                        zn.getPath()
                ), () -> zkMetaStore.createZNodeWithData(zn, data1));
    }


    @Test
    public void testUpdateZNodeWithDataFailure() throws Exception {
        SetDataBuilder builder = spy(zkCuratorFramework.setData());
        doReturn(builder).when(zkCuratorFramework).setData();

        doThrow(new KeeperException.NoNodeException()).when(builder).forPath(any(), any());
        validateException(
                ResourceNotFoundException.class, String.format("%s(%s) not found.", zn.getKind(), zn.getName()),
                () -> zkMetaStore.updateZNodeWithData(zn, data1)
        );


        doThrow(new KeeperException.BadVersionException()).when(builder).forPath(any(), any());
        validateException(
                InvalidOperationForResourceException.class, String.format(
                        "Conflicting update, %s(%s) has been modified. Fetch latest and try again.",
                        zn.getKind(), zn.getName()
                ), () -> zkMetaStore.updateZNodeWithData(zn, data1));


        doThrow(new KeeperException.DataInconsistencyException()).when(builder).forPath(any(), any());
        validateException(
                MetaStoreException.class, String.format("Failed to update %s(%s) at %s.", zn.getKind(), zn.getName(),
                        zn.getPath()
                ), () -> zkMetaStore.updateZNodeWithData(zn, data1));
    }

    @Test
    public void testGetZNodeDataAsPojoFailure() throws Exception {
        GetDataBuilder builder = spy(zkCuratorFramework.getData());
        doReturn(builder).when(zkCuratorFramework).getData();

        doThrow(new KeeperException.NoNodeException()).when(builder).forPath(any());
        validateException(
                ResourceNotFoundException.class, String.format("%s(%s) not found.", zn.getKind(), zn.getName()),
                () -> zkMetaStore.getZNodeDataAsPojo(zn, TestData.class)
        );

        doThrow(new KeeperException.AuthFailedException()).when(builder).forPath(any());
        validateException(
                MetaStoreException.class,
                String.format("Failed to find %s(%s) at %s.", zn.getKind(), zn.getName(), zn.getPath()),
                () -> zkMetaStore.getZNodeDataAsPojo(zn, TestData.class)
        );
    }

    @Test
    public void testZkPathExistFailure() throws Exception {
        ExistsBuilder builder = spy(zkCuratorFramework.checkExists());
        doReturn(builder).when(zkCuratorFramework).checkExists();

        doThrow(new KeeperException.AuthFailedException()).when(builder).forPath(any());
        validateException(
                MetaStoreException.class,
                String.format("Failed to check if %s(%s) exists.", zn.getName(), zn.getPath()),
                () -> zkMetaStore.zkPathExist(zn)
        );
    }

    @Test
    public void testDeleteZNodeFailure() throws Exception {
        DeleteBuilder builder = spy(zkCuratorFramework.delete());
        doReturn(builder).when(zkCuratorFramework).delete();

        doThrow(new KeeperException.NoNodeException()).when(builder).forPath(zn.getPath());
        validateException(
                ResourceNotFoundException.class, String.format("%s(%s) not found.", zn.getKind(), zn.getName()),
                () -> zkMetaStore.deleteZNode(zn)
        );

        doThrow(new KeeperException.InvalidACLException()).when(builder).forPath(zn.getPath());
        validateException(
                MetaStoreException.class,
                String.format("Failed to delete %s(%s) at %s.", zn.getKind(), zn.getName(), zn.getPath()),
                () -> zkMetaStore.deleteZNode(zn)
        );
    }

    @Test
    public void testListChildrenFailure() throws Exception {
        ZNode zn1 = ZNode.OfEntityType(testKind);
        GetChildrenBuilder builder = spy(zkCuratorFramework.getChildren());
        doReturn(builder).when(zkCuratorFramework).getChildren();

        doThrow(new KeeperException.InvalidCallbackException()).when(builder).forPath(zn1.getPath());
        validateException(
                MetaStoreException.class,
                String.format("Failed to list children for entity type %s at path %s.", zn1.getName(),
                        zn1.getPath()
                ),
                () -> zkMetaStore.listChildren(zn1)
        );

        ZNodeKind testKind2 = new ZNodeKind("test2");
        ZNode zn2 = ZNode.OfEntityType(testKind2);
        ResourceNotFoundException e =
                Assertions.assertThrows(ResourceNotFoundException.class, () -> zkMetaStore.listChildren(zn2));
        Assertions.assertEquals(
                String.format("Path(%s) not found for entity %s.", zn2.getPath(), zn2.getName()), e.getMessage());
    }

    private <T extends Exception> void validateException(Class<T> clazz, String errorMsg, MethodCaller caller) {
        T e = Assertions.assertThrows(clazz, caller::call);
        Assertions.assertEquals(errorMsg, e.getMessage());
    }

    interface MethodCaller {
        void call();
    }

    @Setter
    @Getter
    @EqualsAndHashCode(callSuper = true)
    public static class TestData extends MetaStoreEntity {

        String data;

        public TestData(String name, int version, String data) {
            super(name, version);
            this.data = data;
        }
    }
}
