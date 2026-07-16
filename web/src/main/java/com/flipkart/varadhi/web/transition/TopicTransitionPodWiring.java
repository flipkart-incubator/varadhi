package com.flipkart.varadhi.web.transition;

import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.ResourceReadCacheRegistry;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.MessageRouter;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.controller.ControllerConsumerClient;
import com.flipkart.varadhi.core.cluster.failover.TransitionBusAddress;
import com.flipkart.varadhi.core.config.ProducerOptions;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.produce.ProducerService;
import com.flipkart.varadhi.produce.failover.PodTransitionConfig;
import com.flipkart.varadhi.produce.failover.ProduceTransitionMsgHandler;
import com.flipkart.varadhi.produce.failover.TransitionMetrics;
import com.flipkart.varadhi.common.utils.HostUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Wires the pod-side topic-transition stage handler on the cluster broadcast bus.
 */
@Slf4j
public final class TopicTransitionPodWiring {

    private TopicTransitionPodWiring() {
    }

    /**
     * Scheduler and other resources created for topic-transition handling on this pod.
     * The caller that invoked {@link #install} must {@link #close()} it during shutdown.
     */
    public static final class Handle implements AutoCloseable {
        private final ScheduledExecutorService scheduler;

        private Handle(ScheduledExecutorService scheduler) {
            this.scheduler = scheduler;
        }

        @Override
        public void close() {
            scheduler.shutdownNow();
        }
    }

    /**
     * Installs {@link ProduceTransitionMsgHandler} when a cluster manager is configured.
     *
     * @return a closeable handle, or {@code null} when installation was skipped
     */
    public static Handle install(
        VaradhiClusterManager clusterManager,
        Vertx vertx,
        ResourceReadCacheRegistry cacheRegistry,
        ProducerService producerService,
        ProducerOptions producerOptions,
        MeterRegistry meterRegistry
    ) {
        if (clusterManager == null) {
            log.info("Skipping topic-transition stage handler: no cluster manager configured (produce-only mode)");
            return null;
        }
        MessageRouter messageRouter = clusterManager.getRouter(vertx);
        MessageExchange messageExchange = clusterManager.getExchange(vertx);
        ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache = cacheRegistry.getCache(
            ResourceType.TOPIC
        );
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "topic-transition-version-wait")
        );
        ProduceTransitionMsgHandler handler = new ProduceTransitionMsgHandler(
            HostUtils.getHostName(),
            topicCache,
            new ControllerConsumerClient(messageExchange),
            producerService,
            new PodTransitionConfig(
                producerOptions.getTransitionVersionWaitMs(),
                producerOptions.getTransitionPollIntervalMs()
            ),
            scheduler,
            new TransitionMetrics(meterRegistry)
        );
        messageRouter.registerPublishReceiveHandler(
            TransitionBusAddress.ROUTE_TOPIC_TRANSITION,
            TransitionBusAddress.EVENT_PUBLISH_API,
            handler
        );
        log.info("Installed topic-transition stage handler");
        return new Handle(scheduler);
    }
}
