package com.flipkart.varadhi.cluster;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.flipkart.varadhi.core.cluster.MessageHandler;
import com.flipkart.varadhi.core.cluster.messages.ClusterMessage;
import com.flipkart.varadhi.core.cluster.messages.ResponseMessage;
import com.flipkart.varadhi.utils.JsonMapper;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.spi.cluster.zookeeper.ZookeeperClusterManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.spy;


@ExtendWith(VertxExtension.class)
public class MessageChannelImplTest {

    CuratorFramework zkCuratorFramework;

    // TODO:: Tests needs to be added, so this will go under refactor
    @BeforeEach
    public void setup() {
        JsonMapper.getMapper().registerSubtypes(new NamedType(TestClusterMessage.class, "TestClusterMessage"));
        JsonMapper.getMapper()
                .registerSubtypes(new NamedType(ExtendedTestClusterMessage.class, "ExtendedTestClusterMessage"));
    }

    private Vertx createClusteredVertx() throws Exception {
        TestingServer zkCuratorTestingServer = new TestingServer();
        zkCuratorFramework = spy(
                CuratorFrameworkFactory.newClient(
                        zkCuratorTestingServer.getConnectString(), new ExponentialBackoffRetry(1000, 1)));
        zkCuratorFramework.start();
        ClusterManager cm = new ZookeeperClusterManager(zkCuratorFramework, "foo");
        return Vertx.builder().withClusterManager(cm).buildClustered().toCompletionStage().toCompletableFuture().get();
    }

    @Test
    public void sendMessageNoConsumer(VertxTestContext testContext) throws Exception {
        Checkpoint checkpoint = testContext.checkpoint(1);
        Vertx vertx = createClusteredVertx();
        MessageChannelImpl c = new MessageChannelImpl(vertx.eventBus());
        ClusterMessage cm = getClusterMessage("foo");
        Future.fromCompletionStage(c.send("foo", cm)).onComplete(testContext.failing(v -> checkpoint.flag()));
    }

//    @Test
//    public void testSendMessageConsumerCollocated(VertxTestContext testContext) throws Exception {
//        Checkpoint checkpoint = testContext.checkpoint(2);
//        Vertx vertx = createClusteredVertx();
//        MessageChannelImpl c = new MessageChannelImpl(vertx.eventBus());
//        c.addMessageHandler("foo", new MessageHandler() {
//            @Override
//            public <E extends ClusterMessage> CompletableFuture<Void> handle(E message) {
//                if (message instanceof ExtendedTestClusterMessage) {
//                    checkpoint.flag();
//                }
//                return CompletableFuture.completedFuture(null);
//            }
//
//            @Override
//            public <E extends ClusterMessage> CompletableFuture<ResponseMessage> request(E message) {
//                return null;
//            }
//        });
//
//        ClusterMessage cm = getClusterMessage("foo");
//        Future.fromCompletionStage(c.send("foo", cm)).onComplete(testContext.succeeding(v -> checkpoint.flag()));
//    }

    ClusterMessage getClusterMessage(String data) {
        ExtendedTestClusterMessage dm = new ExtendedTestClusterMessage();
        dm.data1 = data;
        dm.data2 = data;
        return dm;
    }

    public static class TestClusterMessage extends ClusterMessage {
        String data1;
    }

    public static class ExtendedTestClusterMessage extends TestClusterMessage {
        String data2;
    }


}
