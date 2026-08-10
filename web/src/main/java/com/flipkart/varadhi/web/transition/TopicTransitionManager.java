package com.flipkart.varadhi.web.transition;

import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.ResourceReadCacheRegistry;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.MessageRouter;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.controller.ControllerRemoteClient;
import com.flipkart.varadhi.core.cluster.failover.TransitionEventSubscriber;
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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Wires the pod-side topic-transition stage handler on the cluster broadcast bus and owns the
 * version-wait + transition executors it creates.
 */
@Slf4j
public final class TopicTransitionManager implements AutoCloseable {

    private final ScheduledExecutorService versionWaitScheduler;
    private final ExecutorService transitionExecutor;
    private final TransitionMetrics metrics;

    private TopicTransitionManager(
        ScheduledExecutorService versionWaitScheduler,
        ExecutorService transitionExecutor,
        TransitionMetrics metrics
    ) {
        this.versionWaitScheduler = versionWaitScheduler;
        this.transitionExecutor = transitionExecutor;
        this.metrics = metrics;
    }

    /**
     * Subscribes {@link ProduceTransitionMsgHandler} via {@link TransitionEventSubscriber}.
     *
     * @return a closeable wiring handle that owns the transition executors and metrics
     */
    public static TopicTransitionManager wire(
        VaradhiClusterManager clusterManager,
        Vertx vertx,
        ResourceReadCacheRegistry cacheRegistry,
        ProducerService producerService,
        ProducerOptions producerOptions,
        MeterRegistry meterRegistry
    ) {
        MessageRouter messageRouter = clusterManager.getRouter(vertx);
        MessageExchange messageExchange = clusterManager.getExchange(vertx);
        ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache = cacheRegistry.getCache(
            ResourceType.TOPIC
        );
        ScheduledExecutorService versionWaitScheduler = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "topic-transition-version-wait")
        );
        ExecutorService transitionExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "topic-transition"));
        TransitionMetrics metrics = new TransitionMetrics(meterRegistry);
        ProduceTransitionMsgHandler handler = new ProduceTransitionMsgHandler(
            HostUtils.getHostName(),
            topicCache,
            new ControllerRemoteClient(messageExchange),
            producerService,
            new PodTransitionConfig(
                producerOptions.getTransitionVersionWaitMs(),
                producerOptions.getTransitionPollIntervalMs()
            ),
            versionWaitScheduler,
            transitionExecutor,
            metrics
        );
        TransitionEventSubscriber.subscribe(messageRouter, handler);
        log.info("Wired topic-transition stage handler");
        return new TopicTransitionManager(versionWaitScheduler, transitionExecutor, metrics);
    }

    @Override
    public void close() {
        versionWaitScheduler.shutdownNow();
        transitionExecutor.shutdownNow();
        metrics.close();
    }
}
