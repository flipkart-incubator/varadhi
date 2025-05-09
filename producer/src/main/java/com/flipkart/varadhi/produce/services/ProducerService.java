package com.flipkart.varadhi.produce.services;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.flipkart.varadhi.common.EntityReadCache;
import com.flipkart.varadhi.common.Result;
import com.flipkart.varadhi.common.exceptions.ProduceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.entities.filters.Condition;
import com.flipkart.varadhi.entities.filters.OrgFilters;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.produce.config.ProducerOptions;
import com.flipkart.varadhi.produce.otel.ProducerMetricsEmitter;
import com.flipkart.varadhi.spi.services.Producer;
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
     * @param varadhiTopicName the full name of the Varadhi topic
     * @param storageTopic     the storage topic instance
     */
    private record ProducerCacheKey(String varadhiTopicName, StorageTopic storageTopic) {
        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            ProducerCacheKey that = (ProducerCacheKey)o;
            return Objects.equals(varadhiTopicName, that.varadhiTopicName) && Objects.equals(
                storageTopic.getName(),
                that.storageTopic.getName()
            );
        }

        @Override
        public int hashCode() {
            return Objects.hash(varadhiTopicName, storageTopic.getName());
        }

        @Override
        public String toString() {
            return "ProducerCacheKey[varadhiTopic=" + varadhiTopicName + ", storageTopic=" + storageTopic.getName()
                   + "]";
        }
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
    private final EntityReadCache<OrgDetails> orgCache;
    /**
     * Cache for Varad
     */
    private final EntityReadCache<Project> projectCache;
    /**
     * Cache for VaradhiTopic entities.
     */

    private final EntityReadCache<VaradhiTopic> topicCache;

    /**
     * Creates a new ProducerService with default options.
     * <p>
     * This constructor uses the default producer options, which include a TTL of 60 minutes
     * for cached producers.
     *
     * @param produceRegion    the region where messages are produced
     * @param producerProvider function to create producers for storage topics
     * @param topicCache       cache for VaradhiTopic entities
     */
    public ProducerService(
        String produceRegion,
        Function<StorageTopic, Producer> producerProvider,
        EntityReadCache<OrgDetails> orgCache,
        EntityReadCache<Project> projectCache,
        EntityReadCache<VaradhiTopic> topicCache
    ) {
        this(produceRegion, producerProvider, orgCache, projectCache, topicCache, ProducerOptions.defaultOptions());
    }

    /**
     * Creates a new ProducerService with the specified options.
     * <p>
     * This constructor allows customization of producer options, such as the TTL for
     * cached producers.
     *
     * @param produceRegion    the region where messages are produced
     * @param producerProvider function to create producers for storage topics
     * @param topicCache       cache for VaradhiTopic entities
     * @param producerOptions  configuration options for producers
     */
    public ProducerService(
        String produceRegion,
        Function<StorageTopic, Producer> producerProvider,
        EntityReadCache<OrgDetails> orgCache,
        EntityReadCache<Project> projectCache,
        EntityReadCache<VaradhiTopic> topicCache,
        ProducerOptions producerOptions
    ) {
        this.produceRegion = produceRegion;
        this.topicCache = topicCache;
        this.projectCache = projectCache;
        this.orgCache = orgCache;
        this.producerCache = Caffeine.newBuilder()
                                     .expireAfterAccess(producerOptions.getProducerCacheTtlSeconds(), TimeUnit.SECONDS)
                                     .recordStats()
                                     .build(key -> producerProvider.apply(key.storageTopic()));
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
     * @param varadhiTopicName the name of the Varadhi topic to produce to
     * @param metricsEmitter   emitter for production metrics
     * @return a future that completes with the result of the produce operation
     * @throws ResourceNotFoundException if the topic does not exist or is not available in the region
     * @throws ProduceException          if production fails due to an internal error
     */
    public CompletableFuture<ProduceResult> produceToTopic(
        Message message,
        String varadhiTopicName,
        ProducerMetricsEmitter metricsEmitter
    ) {
        Optional<VaradhiTopic> topic = topicCache.get(varadhiTopicName);

        if (topic.isEmpty() || !topic.get().isActive()) {
            throw new ResourceNotFoundException(
                "Topic(%s) ".formatted(varadhiTopicName) + (topic.isEmpty() ? "does not exist" : "is not active")
            );
        }

        return produceToValidTopic(message, topic.get(), metricsEmitter);
    }

    /**
     * Produces a message to a valid Varadhi topic.
     *
     * @param message        the message to produce
     * @param varadhiTopic   the Varadhi topic to produce to
     * @param metricsEmitter emitter for production metrics
     * @return a future that completes with the result of the produce operation
     * @throws ResourceNotFoundException if the topic is not available in the region
     * @throws ProduceException          if production fails due to an internal error
     */
    private CompletableFuture<ProduceResult> produceToValidTopic(
        Message message,
        VaradhiTopic varadhiTopic,
        ProducerMetricsEmitter metricsEmitter
    ) {
        InternalCompositeTopic internalTopic = varadhiTopic.getProduceTopicForRegion(produceRegion);

        if (internalTopic == null) {
            throw new ResourceNotFoundException(String.format("Topic not found for region(%s).", produceRegion));
        }

        if (!internalTopic.getTopicState().isProduceAllowed()) {
            return CompletableFuture.completedFuture(
                ProduceResult.ofNonProducingTopic(message.getMessageId(), internalTopic.getTopicState())
            );
        }

        if (applyOrgFilter(varadhiTopic.getProjectName(), varadhiTopic.getName(), message)) {
            return CompletableFuture.completedFuture(ProduceResult.ofFilteredMessage(message.getMessageId()));
        }

        StorageTopic storageTopic = internalTopic.getTopicToProduce();
        return getProducer(storageTopic, varadhiTopic.getName()).thenCompose(
            producer -> produceToStorageProducer(producer, metricsEmitter, storageTopic.getName(), message).thenApply(
                result -> ProduceResult.of(message.getMessageId(), result)
            )
        );
    }

    /**
     * Gets a producer for the specified storage topic.
     * <p>
     * This method first checks if the producer is already in the cache. If not, it attempts
     * to load it using the producer provider function.
     *
     * @param storageTopic     the storage topic to get a producer for
     * @param varadhiTopicName the name of the Varadhi topic (used for caching)
     * @return a future that completes with the producer
     */
    public CompletableFuture<Producer> getProducer(StorageTopic storageTopic, String varadhiTopicName) {
        ProducerCacheKey key = new ProducerCacheKey(varadhiTopicName, storageTopic);
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
     * @param metricsEmitter emitter for production metrics
     * @param topicName      the name of the storage topic
     * @param message        the message to produce
     * @return a future that completes with the result of the produce operation
     */
    private CompletableFuture<Result<Offset>> produceToStorageProducer(
        Producer producer,
        ProducerMetricsEmitter metricsEmitter,
        String topicName,
        Message message
    ) {
        Instant startTime = Instant.now();
        return producer.produceAsync(message).handle((result, throwable) -> {
            Duration latency = Duration.between(startTime, Instant.now());
            metricsEmitter.emit(result != null, latency.toMillis());
            if (throwable != null) {
                log.debug(
                    "Produce Message({}) to StorageTopic({}) failed.",
                    message.getMessageId(),
                    topicName,
                    throwable
                );
            }
            return Result.of(result, throwable);
        });
    }

    private boolean applyOrgFilter(String projectName, String topicName, Message message) {
        Optional<Project> projectOptional = projectCache.get(projectName);
        if (projectOptional.isEmpty()) {
            return false;
        }
        Project project = projectOptional.get();

        Optional<VaradhiTopic> topicOptional = topicCache.get(VaradhiTopic.buildTopicName(projectName, topicName));
        if (topicOptional.isEmpty()) {
            return false;
        }
        VaradhiTopic varadhiTopic = topicOptional.get();

        Optional<OrgDetails> orgDetailsOptional = orgCache.get(project.getOrg());
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
