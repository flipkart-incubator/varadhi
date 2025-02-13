package com.flipkart.varadhi.cluster;

import com.flipkart.varadhi.cluster.custom.VaradhiZkClusterManager;
import com.flipkart.varadhi.cluster.messages.ClusterMessage;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;


@ExtendWith (VertxExtension.class)
public class MessageRouterImplTest {

    private TestingServer zkCuratorTestingServer;
    private CuratorFramework zkCuratorFramework;
    private VaradhiZkClusterManager vZkCm;

    // TODO:: Tests needs to be added, so this will go under refactor
    @BeforeEach
    public void setup() throws Exception {
        zkCuratorTestingServer = new TestingServer();
        zkCuratorFramework = CuratorFrameworkFactory.newClient(
            zkCuratorTestingServer.getConnectString(),
            new ExponentialBackoffRetry(1000, 1)
        );
        zkCuratorFramework.start();
        vZkCm = new VaradhiZkClusterManager(zkCuratorFramework, new DeliveryOptions(), "localhost");
    }

    @AfterEach
    public void tearDown() throws Exception {
        zkCuratorFramework.close();
        zkCuratorTestingServer.close();
    }

    private Vertx createClusteredVertx() throws Exception {
        return Vertx.builder()
                    .withClusterManager(vZkCm)
                    .buildClustered()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get();
    }

    //    @Test
    //    public void sendMessageNoConsumer(VertxTestContext testContext) throws Exception {
    //        Checkpoint checkpoint = testContext.checkpoint(1);
    //        Vertx vertx = createClusteredVertx();
    //        MessageExchange me = vZkCm.getExchange(vertx);
    //        ClusterMessage cm = getClusterMessage("foo");
    //        Future.fromCompletionStage(me.send("foo", "start", cm)).onComplete(testContext.failing(v -> checkpoint.flag()));
    //    }

    @Test
    public void testSendMessageConsumerCollocated(VertxTestContext testContext) throws Exception {
        Checkpoint checkpoint = testContext.checkpoint(2);
        Vertx vertx = createClusteredVertx();
        MessageExchange me = vZkCm.getExchange(vertx);
        MessageRouter mr = vZkCm.getRouter(vertx);
        mr.sendHandler("testAddress", "customApi", message -> checkpoint.flag());
        ClusterMessage cm = getClusterMessage("foo");
        Future.fromCompletionStage(me.send("testAddress", "customApi", cm))
              .onComplete(testContext.succeeding(v -> checkpoint.flag()));
    }

    ClusterMessage getClusterMessage(String data) {
        return ClusterMessage.of(data);
    }
}
