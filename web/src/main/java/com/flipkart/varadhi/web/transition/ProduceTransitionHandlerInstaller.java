package com.flipkart.varadhi.web.transition;

import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.core.ResourceReadCacheRegistry;
import com.flipkart.varadhi.entities.ResourceType;
import com.flipkart.varadhi.core.cluster.MessageExchange;
import com.flipkart.varadhi.core.cluster.MessageRouter;
import com.flipkart.varadhi.core.cluster.VaradhiClusterManager;
import com.flipkart.varadhi.core.cluster.failover.TransitionBusAddress;
import com.flipkart.varadhi.core.config.ProducerOptions;
import com.flipkart.varadhi.entities.RegionName;
import com.flipkart.varadhi.entities.Resource;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.VaradhiTopicName;
import com.flipkart.varadhi.entities.cluster.failover.TransitionType;
import com.flipkart.varadhi.produce.ProducerService;
import com.flipkart.varadhi.produce.failover.ControllerTransitionAckClient;
import com.flipkart.varadhi.produce.failover.PodTransitionConfig;
import com.flipkart.varadhi.produce.failover.ProduceTransitionMsgHandler;
import com.flipkart.varadhi.produce.failover.TransitionMetricsImpl;
import com.flipkart.varadhi.produce.failover.TransitionPrepareResult;
import com.flipkart.varadhi.common.utils.HostUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;

/**
 * Wires the pod-side topic-transition stage handler onto the cluster broadcast bus.
 */
@Slf4j
public final class ProduceTransitionHandlerInstaller {

    private ProduceTransitionHandlerInstaller() {
    }

    /**
     * Registers {@link ProduceTransitionMsgHandler} when a cluster manager is configured.
     *
     * @return the scheduler used for version polling, or {@code null} when installation was skipped
     */
    public static ScheduledExecutorService install(
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
            new ControllerTransitionAckClient(messageExchange),
            producerService,
            buildPrepareActions(producerService),
            new PodTransitionConfig(
                producerOptions.getTransitionVersionWaitMs(),
                producerOptions.getTransitionPollIntervalMs()
            ),
            scheduler,
            new TransitionMetricsImpl(meterRegistry)
        );
        messageRouter.registerPublishHandler(
            TransitionBusAddress.ROUTE_TOPIC_TRANSITION,
            TransitionBusAddress.EVENT_PUBLISH_API,
            handler
        );
        log.info("Registered topic-transition stage handler");
        return scheduler;
    }

    private static Map<TransitionType, BiFunction<VaradhiTopicName, String, CompletableFuture<TransitionPrepareResult>>> buildPrepareActions(
        ProducerService producerService
    ) {
        BiFunction<VaradhiTopicName, String, CompletableFuture<TransitionPrepareResult>> failoverWarm = (
            topicName,
            targetRegion
        ) -> producerService.getProducer(topicName, RegionName.of(targetRegion))
                            .thenApply(producer -> TransitionPrepareResult.INVOLVED);
        return Map.of(TransitionType.TOPIC_FAILOVER, failoverWarm);
    }
}
