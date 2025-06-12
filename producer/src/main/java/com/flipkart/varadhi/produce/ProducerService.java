package com.flipkart.varadhi.produce;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.flipkart.varadhi.core.ResourceReadCache;
import com.flipkart.varadhi.common.Result;
import com.flipkart.varadhi.core.exceptions.ProduceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.filters.Condition;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import com.flipkart.varadhi.core.config.ProducerOptions;
import com.flipkart.varadhi.produce.telemetry.ProducerMetrics;
import com.flipkart.varadhi.spi.services.Producer;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;

/**
 * Service responsible for producing messages to topics in Varadhi.
 * <p>
 * This service provides high-performance, thread-safe message production capabilities with:
 * <ul>
 *   <li>Efficient producer caching using Caffeine for optimal resource utilization</li>
 *   <li>Fully asynchronous message production with CompletableFuture</li>
 *   <li>Comprehensive metrics collection for monitoring and performance analysis</li>
 *   <li>Robust error handling with detailed error messages</li>
 * </ul>
 * <p>
 * The service maintains a cache of producers for storage topics to optimize performance
 * and resource utilization. Producers are created on-demand and cached for reuse based on
 * configurable TTL settings.
 */
@Slf4j
public final class ProducerService {

    /**
     * A record that serves as a cache key for producers.
     *
     * @param varadhiTopicFQN the full name of the Varadhi topic
     * @param storageTopicId     the storage topic id
     */
    private record ProducerCacheKey(String varadhiTopicFQN, int storageTopicId) {
    }

    /**
     * Cache of producers for storage topics.
     */
    private final LoadingCache<ProducerCacheKey, Producer> producerCache;

    /**
     * The region where messages are produced.
     */
    private final String produceRegion;

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

    /**
     * Creates a new ProducerService with default options.
     * <p>
     * This constructor uses the default producer options, which include a TTL of 60 minutes
     * for cached producers.
     *
     * @param produceRegion    the region where messages are produced
     * @param producerFactory function to create producers for storage topics
     * @param topicCache       cache for VaradhiTopic resource
     */
    public ProducerService(
        String produceRegion,
        ProducerFactory<StorageTopic> producerFactory,
        ResourceReadCache<OrgDetails> orgCache,
        ResourceReadCache<Resource.EntityResource<Project>> projectCache,
        ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache

    ) {
        this(
            produceRegion,
            producerFactory,
            orgCache,
            projectCache,
            topicCache,
            t -> ProducerMetrics.NOOP,
            ProducerOptions.defaultOptions()
        );
    }

    /**
     * Creates a new ProducerService with the specified options.
     * <p>
     * This constructor allows customization of producer options, such as the TTL for
     * cached producers.
     *
     * @param produceRegion    the region where messages are produced
     * @param producerFactory function to create producers for storage topics
     * @param topicCache       cache for VaradhiTopic resource
     * @param producerOptions  configuration options for producers
     */
    public ProducerService(
        String produceRegion,
        ProducerFactory<StorageTopic> producerFactory,
        ResourceReadCache<OrgDetails> orgCache,
        ResourceReadCache<Resource.EntityResource<Project>> projectCache,
        ResourceReadCache<Resource.EntityResource<VaradhiTopic>> topicCache,
        Function<String, ProducerMetrics> metricsRecorderProvider,
        ProducerOptions producerOptions
    ) {
        this.produceRegion = produceRegion;
        this.topicCache = topicCache;
        this.projectCache = projectCache;
        this.orgCache = orgCache;
        this.producerCache = Caffeine.newBuilder()
                                     .expireAfterAccess(producerOptions.getProducerCacheTtlSeconds(), TimeUnit.SECONDS)
                                     .recordStats()
                                     .build(key -> loadProducerObject(produceRegion, producerFactory, key));
        this.metricsProvider = metricsRecorderProvider;
    }

    private Producer loadProducerObject(
        String produceRegion,
        ProducerFactory<StorageTopic> producerFactory,
        ProducerCacheKey key
    ) {
        var topicMaybe = topicCache.get(key.varadhiTopicFQN);
        if (topicMaybe.isEmpty()) {
            throw new ResourceNotFoundException(
                "Topic(%s) does not exist in region(%s).".formatted(key.varadhiTopicFQN, produceRegion)
            );
        }

        var topic = topicMaybe.get();

        return producerFactory.newProducer(
            topic.getEntity().getProduceTopicForRegion(produceRegion).getTopic(key.storageTopicId),
            topic.getEntity().getCapacity()
        );
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

        return produceToValidTopic(topic.get().getEntity(), message).whenComplete(metrics::accepted);
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
        SegmentedStorageTopic internalTopic = topic.getProduceTopicForRegion(produceRegion);

        if (internalTopic == null) {
            throw new ResourceNotFoundException(String.format("Topic not found for region(%s).", produceRegion));
        }

        if (!internalTopic.getTopicState().isProduceAllowed()) {
            return CompletableFuture.completedFuture(
                ProduceResult.ofNonProducingTopic(message.getMessageId(), internalTopic.getTopicState())
            );
        }

        if (applyOrgFilter(topic, message)) {
            return CompletableFuture.completedFuture(ProduceResult.ofFilteredMessage(message.getMessageId()));
        }

        StorageTopic storageTopic = internalTopic.getTopicToProduce();
        return getProducer(topic.getName(), storageTopic).thenCompose(
            producer -> doProduce(producer, storageTopic.getName(), message)
        );
    }

    /**
     * Gets a producer for the specified storage topic.
     * <p>
     * This method first checks if the producer is already in the cache. If not, it attempts
     * to load it using the producer provider function.
     *
     * @param topicFQN the name of the Varadhi topic (used for caching)
     * @param storageTopic   the storage topic to get a producer for
     * @return a future that completes with the producer
     */
    public CompletableFuture<Producer> getProducer(String topicFQN, StorageTopic storageTopic) {
        ProducerCacheKey key = new ProducerCacheKey(topicFQN, storageTopic.getId());
        Producer producer = producerCache.getIfPresent(key);
        if (producer != null) {
            return CompletableFuture.completedFuture(producer);
        }

        try {
            return CompletableFuture.completedFuture(producerCache.get(key));
        } catch (Exception e) {
            String errorMsg = String.format(
                "Error getting producer for Topic(%s): %s",
                storageTopic.getName(),
                e.getMessage()
            );
            return CompletableFuture.failedFuture(new ProduceException(errorMsg, e));
        }
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
    private CompletableFuture<ProduceResult> doProduce(Producer producer, String topicName, Message message) {
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
            var result = ProduceResult.of(message.getMessageId(), Result.of(offset, throwable));
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
