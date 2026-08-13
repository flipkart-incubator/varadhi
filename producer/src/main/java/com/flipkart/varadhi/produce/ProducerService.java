package com.flipkart.varadhi.produce;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.common.Result;
import com.flipkart.varadhi.common.exceptions.ProduceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.filters.Condition;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import com.flipkart.varadhi.core.config.ProducerOptions;
import com.flipkart.varadhi.produce.ratelimit.ProduceRateLimiter;
import com.flipkart.varadhi.produce.telemetry.ProducerMetrics;
import com.flipkart.varadhi.spi.services.Producer;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import lombok.extern.slf4j.Slf4j;

/**
 * Service responsible for producing messages to topics in Varadhi.
 * <p>
 * Producers are cached by {@link ProduceKey} in an event-driven registry: entries are removed
 * (and {@link Producer#close()}'d) on topic invalidate / delete only — not on ordinary topic
 * upserts — so PREPARE-warmed producers survive FENCE/MIGRATE version bumps.
 */
@Slf4j
public final class ProducerService {

    /**
     * Registry of producers for {@link ProduceKey}s.
     */
    private final ConcurrentHashMap<ProduceKey, Producer<? extends Offset>> producerCache;

    private final ProducerFactory producerFactory;

    /**
     * The region this pod is deployed in (ingress / local region).
     */
    private final String localRegion;

    /**
     * Cache for Varad
     */
    private final ResourceReadCache<OrgDetails> orgCache;
    /**
     * Cache for Varad
     */
    private final ResourceReadCache<Resource.EntityResource<Project>> projectCache;
    /**
     * Cache for VaradhiTopic resource.
     */

    private final ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache;

    private final Map<String, ProducerMetrics> metrics = new ConcurrentHashMap<>();
    private final Function<String, ProducerMetrics> metricsProvider;
    private final ProduceRateLimiter rateLimiter;

    /**
     * Creates a new ProducerService with default options.
     * <p>
     * This constructor uses the default producer options, which include a TTL of 60 minutes
     * for cached producers.
     *
     * @param localRegion      the region this pod is deployed in
     * @param producerFactory function to create producers for storage topics
     * @param topicCache       cache for VaradhiTopic resource
     */
    // TODO: fix the generic type parameters. See ProduceBenchmarkTest for the issue.
    public ProducerService(
        String localRegion,
        ProducerFactory producerFactory,
        ResourceReadCache<OrgDetails> orgCache,
        ResourceReadCache<Resource.EntityResource<Project>> projectCache,
        ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache

    ) {
        this(
            localRegion,
            producerFactory,
            orgCache,
            projectCache,
            topicCache,
            t -> ProducerMetrics.NOOP,
            ProducerOptions.defaultOptions(),
            ProduceRateLimiter.disabled()
        );
    }

    /**
     * Creates a new ProducerService with the specified options.
     * <p>
     * This constructor allows customization of producer options, such as the TTL for
     * cached producers.
     *
     * @param localRegion      the region this pod is deployed in
     * @param producerFactory function to create producers for storage topics
     * @param topicCache       cache for VaradhiTopic resource
     * @param producerOptions  configuration options for producers
     */
    public ProducerService(
        String localRegion,
        ProducerFactory producerFactory,
        ResourceReadCache<OrgDetails> orgCache,
        ResourceReadCache<Resource.EntityResource<Project>> projectCache,
        ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache,
        Function<String, ProducerMetrics> metricsRecorderProvider,
        ProducerOptions producerOptions
    ) {
        this(
            localRegion,
            producerFactory,
            orgCache,
            projectCache,
            topicCache,
            metricsRecorderProvider,
            producerOptions,
            ProduceRateLimiter.disabled()
        );
    }

    public ProducerService(
        String localRegion,
        ProducerFactory producerFactory,
        ResourceReadCache<OrgDetails> orgCache,
        ResourceReadCache<Resource.EntityResource<Project>> projectCache,
        ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache,
        Function<String, ProducerMetrics> metricsRecorderProvider,
        ProducerOptions producerOptions,
        ProduceRateLimiter rateLimiter
    ) {
        this.localRegion = localRegion;
        this.topicCache = topicCache;
        this.projectCache = projectCache;
        this.orgCache = orgCache;
        this.rateLimiter = rateLimiter;
        this.producerFactory = producerFactory;
        this.producerCache = new ConcurrentHashMap<>();
        this.metricsProvider = metricsRecorderProvider;
        topicCache.addOnInvalidate(this::evictProducersForTopic);
    }

    private Producer<? extends Offset> loadProducerObject(ProduceKey key) {
        var topicMaybe = topicCache.get(key.topicFqn().toFqn());
        if (topicMaybe.isEmpty()) {
            throw new ResourceNotFoundException(
                "Topic(%s) does not exist in region(%s).".formatted(key.topicFqn().toFqn(), key.produceRegion())
            );
        }

        var topic = topicMaybe.get();

        return producerFactory.newProducer(
            topic.getEntity().getSegmentedStorageTopic().getTopic(key.storageTopicId()),
            topic.getEntity().getCapacity(),
            key.produceRegion().value()
        );
    }

    private void evictProducersForTopic(String topicFqn) {
        VaradhiTopicName name;
        try {
            name = VaradhiTopicName.parse(topicFqn);
        } catch (RuntimeException e) {
            log.warn("Ignoring producer eviction for unparseable topic FQN {}", topicFqn, e);
            return;
        }
        VaradhiTopicName topicName = name;
        producerCache.entrySet().removeIf(entry -> {
            if (!entry.getKey().topicFqn().equals(topicName)) {
                return false;
            }
            closeQuietly(entry.getValue());
            return true;
        });
    }

    private static void closeQuietly(Producer<? extends Offset> producer) {
        try {
            producer.close();
        } catch (Exception e) {
            log.warn("Failed to close producer on registry eviction", e);
        }
    }

    private ProducerMetrics getMetrics(String topicFQN) {
        return metrics.computeIfAbsent(topicFQN, metricsProvider);
    }

    /**
     * Produces a message to the specified Varadhi topic.
     * <p>
     * This method handles the entire process of producing a message, including:
     * <ul>
     *   <li>Validating the topic exists and is active</li>
     *   <li>Checking if production is allowed to the topic</li>
     *   <li>Obtaining the appropriate producer</li>
     *   <li>Sending the message asynchronously</li>
     *   <li>Collecting metrics</li>
     * </ul>
     *
     * @param message          the message to produce
     * @param topicFQN the name of the Varadhi topic to produce to
     * @return a future that completes with the result of the produce operation
     * @throws ResourceNotFoundException if the topic does not exist or is not available in the region
     * @throws ProduceException          if production fails due to an internal error
     */
    public CompletableFuture<ProduceResult> produceToTopic(Message message, String topicFQN) {
        Optional<Resource.EntityResource<VaradhiTopic>> topic = topicCache.get(topicFQN);

        if (topic.isEmpty() || !topic.get().getEntity().isActive()) {
            throw new ResourceNotFoundException(
                "Topic(%s) ".formatted(topicFQN) + (topic.isEmpty() ? "does not exist" : "is not active")
            );
        }

        ProducerMetrics metrics = getMetrics(topicFQN);
        metrics.received(message.getPayload().length, message.getTotalSizeBytes());

        return produceToValidTopic(topic.get().getEntity(), message).whenComplete(
            (result, t) -> metrics.accepted(result, t, message.getTotalSizeBytes())
        );
    }

    /**
     * Produces a message to a valid Varadhi topic.
     *
     * @param message   the message to produce
     * @param topic     the Varadhi topic to produce to
     *
     * @return a future that completes with the result of the produce operation
     * @throws ResourceNotFoundException if the topic is not available in the region
     * @throws ProduceException          if production fails due to an internal error
     */
    private CompletableFuture<ProduceResult> produceToValidTopic(VaradhiTopic topic, Message message) {
        RegionName deployed = RegionName.of(localRegion);
        Optional<ProduceKey> produceKey = TopicResolver.resolve(topic, deployed, true);
        if (produceKey.isEmpty()) {
            return topic.getProduceConfig(deployed)
                        .map(
                            config -> CompletableFuture.completedFuture(
                                ProduceResult.ofNonProducingTopic(message.getMessageId(), config.getState())
                            )
                        )
                        .orElseThrow(
                            () -> new ResourceNotFoundException(
                                "Topic(%s) is not available in region(%s).".formatted(topic.getName(), localRegion)
                            )
                        );
        }

        if (applyOrgFilter(topic, message)) {
            return CompletableFuture.completedFuture(ProduceResult.ofFilteredMessage(message.getMessageId()));
        }

        if (rateLimiter.check(topic, message.getTotalSizeBytes())) {
            return CompletableFuture.completedFuture(ProduceResult.ofThrottled(message.getMessageId()));
        }

        ProduceKey key = produceKey.get();
        StorageTopic storageTopic = topic.getSegmentedStorageTopic().getTopic(key.storageTopicId());
        return getProducer(key).thenCompose(producer -> doProduce(producer, storageTopic.getName(), message));
    }

    /**
     * Gets a producer for the resolved {@link ProduceKey}.
     */
    public CompletableFuture<Producer<? extends Offset>> getProducer(ProduceKey produceKey) {
        Producer<? extends Offset> producer = producerCache.get(produceKey);
        if (producer != null) {
            return CompletableFuture.completedFuture(producer);
        }

        try {
            return CompletableFuture.completedFuture(
                producerCache.computeIfAbsent(produceKey, this::loadProducerObject)
            );
        } catch (Exception e) {
            String errorMsg = String.format(
                "Error getting producer for Topic(%s): %s",
                produceKey.topicFqn().toFqn(),
                e.getMessage()
            );
            return CompletableFuture.failedFuture(new ProduceException(errorMsg, e));
        }
    }

    /**
     * Pre-warms the producer for {@code region} (ungated resolve). Used by topic-failover PREPARE.
     */
    public CompletableFuture<Producer<? extends Offset>> getProducerForRegion(VaradhiTopic topic, RegionName region) {
        Optional<ProduceKey> key = TopicResolver.resolve(topic, region, false);
        if (key.isEmpty()) {
            return CompletableFuture.failedFuture(
                new ResourceNotFoundException(
                    "Topic(%s) has no produce configuration for region(%s).".formatted(topic.getName(), region.value())
                )
            );
        }
        return getProducer(key.get());
    }

    /**
     * Whether this pod already has a cached producer for the topic's active produce path in
     * {@code localRegion}. Uses <em>ungated</em> resolve (same as PREPARE warm) so fencing does
     * not hide an existing producer when deciding transition participation.
     */
    public boolean hasActiveProducer(VaradhiTopic topic) {
        return TopicResolver.resolve(topic, RegionName.of(localRegion), false)
                            .map(key -> hasProducer(topic.getName(), key.storageTopicId(), key.produceRegion().value()))
                            .orElse(false);
    }

    /**
     * Pre-warms the producer for {@code storageTopicId} in this pod's local region.
     * Used by storage-migration PREPARE.
     */
    public CompletableFuture<Void> loadProducer(VaradhiTopicName topicName, int storageTopicId) {
        ProduceKey key = new ProduceKey(topicName, RegionName.of(localRegion), storageTopicId);
        return getProducer(key).thenRun(() -> {});
    }

    public boolean hasProducer(String topicFQN, int storageTopicId, String region) {
        return producerCache.get(
            new ProduceKey(VaradhiTopicName.parse(topicFQN), RegionName.of(region), storageTopicId)
        ) != null;
    }

    /**
     * Produces a message to a storage topic using the specified producer.
     * <p>
     * This method handles the details of producing to a specific storage topic, including:
     * <ul>
     *   <li>Measuring the latency of the produce operation</li>
     *   <li>Emitting metrics for monitoring</li>
     *   <li>Handling success and failure cases</li>
     * </ul>
     *
     * @param producer       the producer to use
     * @param topicName      the name of the storage topic
     * @param message        the message to produce
     * @return a future that completes with the result of the produce operation
     */
    private CompletableFuture<ProduceResult> doProduce(
        Producer<? extends Offset> producer,
        String topicName,
        Message message
    ) {
        long start = System.currentTimeMillis();
        return producer.produceAsync(message).handle((offset, throwable) -> {
            long latency = System.currentTimeMillis() - start;
            if (throwable != null) {
                log.debug(
                    "Produce Message({}) to StorageTopic({}) failed.",
                    message.getMessageId(),
                    topicName,
                    throwable
                );
            }
            var result = ProduceResult.of(message.getMessageId(), Result.<Offset>of(offset, throwable));
            result.setLatencyMs(latency);
            return result;
        });
    }

    private boolean applyOrgFilter(VaradhiTopic varadhiTopic, Message message) {
        var projectOptional = projectCache.get(varadhiTopic.getProjectName());
        if (projectOptional.isEmpty()) {
            return false;
        }
        Project project = projectOptional.get().getEntity();

        var orgDetailsOptional = orgCache.get(project.getOrg());
        if (orgDetailsOptional.isEmpty()) {
            return false;
        }
        OrgDetails orgDetails = orgDetailsOptional.get();

        String nfrStrategy = varadhiTopic.getNfrFilterName();
        Condition condition = Optional.ofNullable(orgDetails.getOrgFilters())
                                      .map(OrgFilters::getFilters)
                                      .map(filters -> filters.get(nfrStrategy))
                                      .orElse(null);

        return nfrStrategy != null && condition != null && condition.evaluate(message.getHeaders());
    }
}
