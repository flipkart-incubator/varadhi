package com.flipkart.varadhi.db;

import com.flipkart.varadhi.common.exceptions.DuplicateResourceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.cluster.failover.StageSnapshot;
import com.flipkart.varadhi.entities.cluster.failover.TransitionStage;
import com.flipkart.varadhi.entities.cluster.failover.TransitionMaster;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransitionStoreImplTest {

    private static final String FQN = "proj.topic";

    private TestingServer zkServer;
    private CuratorFramework curator;
    private TransitionStoreImpl store;

    @BeforeEach
    void setUp() throws Exception {
        zkServer = new TestingServer();
        curator = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new ExponentialBackoffRetry(1000, 1));
        curator.start();
        store = new TransitionStoreImpl(new ZKMetaStore(curator));
    }

    @AfterEach
    void tearDown() throws Exception {
        curator.close();
        zkServer.close();
    }

    @Test
    void createGetExistsDelete() {
        assertFalse(store.exists(FQN));
        store.create(
            TransitionMaster.forFailover("op-1", FQN, RegionName.of("r1"), RegionName.of("r2"), false, "user")
        );

        assertTrue(store.exists(FQN));
        TransitionMaster got = store.get(FQN);
        assertEquals(FQN, got.getName());
        assertEquals("op-1", got.getOperationId());
        assertEquals(TransitionStage.PENDING, got.getCurrentStage());
        assertFalse(got.isWaitForReplicationLagToClear());
        assertEquals("user", got.getRequestedBy());

        store.delete(FQN);
        assertFalse(store.exists(FQN));
    }

    @Test
    void createIsUniquePerTopic() {
        store.create(
            TransitionMaster.forFailover("op-1", FQN, RegionName.of("r1"), RegionName.of("r2"), false, "user")
        );
        assertThrows(
            DuplicateResourceException.class,
            () -> store.create(
                TransitionMaster.forFailover("op-2", FQN, RegionName.of("r1"), RegionName.of("r2"), false, "user")
            )
        );
    }

    @Test
    void updatePersistsStageAdvance() {
        store.create(
            TransitionMaster.forFailover("op-1", FQN, RegionName.of("r1"), RegionName.of("r2"), false, "user")
        );
        TransitionMaster t = store.get(FQN);
        t.beginStage(TransitionStage.FENCE);
        t.advanceTo(TransitionStage.FENCE, 42L);
        store.update(t);

        TransitionMaster reloaded = store.get(FQN);
        assertEquals(TransitionStage.FENCE, reloaded.getCurrentStage());
        assertEquals(42L, reloaded.getTopicVersionToAwait());
        assertEquals(1, reloaded.getStageHistory().size());
        assertEquals(TransitionStage.FENCE, reloaded.getStageHistory().get(0).getStage());
        assertEquals(StageSnapshot.Outcome.IN_PROGRESS, reloaded.getStageHistory().get(0).getOutcome());
    }

    @Test
    void listActiveReturnsAll() {
        store.create(
            TransitionMaster.forFailover("op-1", "proj.a", RegionName.of("r1"), RegionName.of("r2"), false, "user")
        );
        store.create(
            TransitionMaster.forFailover("op-2", "proj.b", RegionName.of("r1"), RegionName.of("r2"), false, "user")
        );

        List<TransitionMaster> active = store.listActive();
        assertEquals(2, active.size());
    }

    @Test
    void getMissingThrows() {
        assertThrows(ResourceNotFoundException.class, () -> store.get("proj.missing"));
    }
}
