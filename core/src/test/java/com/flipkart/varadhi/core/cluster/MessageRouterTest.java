package com.flipkart.varadhi.core.cluster;

import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.entities.JsonMapper;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;


@ExtendWith (VertxExtension.class)
public class MessageRouterTest {

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

    @Test
    public void testSendHandlerReceivesExactPayload(VertxTestContext testContext) throws Exception {
        // The handler must receive the same message id/data that was sent (round-trip integrity).
        Checkpoint received = testContext.checkpoint(1);
        Checkpoint delivered = testContext.checkpoint(1);
        Vertx vertx = createClusteredVertx();
        MessageExchange me = vZkCm.getExchange(vertx);
        MessageRouter mr = vZkCm.getRouter(vertx);
        ClusterMessage sent = getClusterMessage("payload-1");
        mr.sendHandler("addr", "echo", message -> testContext.verify(() -> {
            Assertions.assertEquals(sent.getId(), message.getId());
            Assertions.assertEquals("payload-1", message.getData(String.class));
            received.flag();
        }));
        Future.fromCompletionStage(me.send("addr", "echo", sent))
              .onComplete(testContext.succeeding(v -> delivered.flag()));
    }

    @Test
    public void testPublishMessageFansOutToAllHandlers(VertxTestContext testContext) throws Exception {
        // publish fans out to every registered handler at the same address.
        Checkpoint checkpoint = testContext.checkpoint(2);
        Vertx vertx = createClusteredVertx();
        MessageRouter mr = vZkCm.getRouter(vertx);
        mr.publishHandler("route", "api", message -> checkpoint.flag());
        mr.publishHandler("route", "api", message -> checkpoint.flag());
        ClusterMessage cm = getClusterMessage("foo");
        vertx.eventBus().publish("route.api.publish", JsonMapper.jsonSerialize(cm));
    }

    @Test
    public void testPublishHandlerSwallowsHandlerExceptions(VertxTestContext testContext) throws Exception {
        // A throwing publish handler must not prevent other handlers at the same address from
        // receiving the message (publish is fire-and-forget; exceptions are only logged).
        Checkpoint healthy = testContext.checkpoint(1);
        Vertx vertx = createClusteredVertx();
        MessageRouter mr = vZkCm.getRouter(vertx);
        mr.publishHandler("route", "api", message -> {
            throw new RuntimeException("boom");
        });
        mr.publishHandler("route", "api", message -> healthy.flag());
        vertx.eventBus().publish("route.api.publish", JsonMapper.jsonSerialize(getClusterMessage("foo")));
    }

    ClusterMessage getClusterMessage(String data) {
        return ClusterMessage.of(data);
    }
}
