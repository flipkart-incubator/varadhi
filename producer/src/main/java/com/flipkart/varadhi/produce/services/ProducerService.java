package com.flipkart.varadhi.produce.services;

import com.flipkart.varadhi.common.Result;
import com.flipkart.varadhi.common.exceptions.ProduceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.produce.config.ProducerOptions;
import com.flipkart.varadhi.produce.otel.ProducerMetricsEmitter;
import com.flipkart.varadhi.produce.providers.TopicProvider;
import com.flipkart.varadhi.spi.services.Producer;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Service responsible for producing messages to topics in Varadhi.
 * <p>
 * This service provides high-performance, thread-safe message production capabilities with:
 * <ul>
 *   <li>Efficient producer caching using Caffeine</li>
 *   <li>Asynchronous message production</li>
 *   <li>Comprehensive metrics collection</li>
 *   <li>Robust error handling</li>
 * </ul>
 * <p>
 * The service maintains a cache of producers for storage topics to optimize performance
 * and resource utilization. Producers are created on-demand and cached for reuse.
 */
@Slf4j
public class ProducerService {

    /**
     * Cache of producers for storage topics.
     */
    private final LoadingCache<StorageTopic, Producer> producerCache;

    /**
     * The region where messages are produced.
     */
    private final String produceRegion;

    /**
     * Provider for topic information.
     */
    private final TopicProvider topicProvider;

    /**
     * Creates a new ProducerService with default options.
     *
     * @param produceRegion    the region where messages are produced
     * @param producerProvider function to create producers for storage topics
     * @param topicProvider    provider for topic information
     * @throws NullPointerException if any parameter is null
     */
    public ProducerService(
        String produceRegion,
        Function<StorageTopic, Producer> producerProvider,
        TopicProvider topicProvider
    ) {
        this(produceRegion, producerProvider, topicProvider, ProducerOptions.defaultOptions());
    }

    /**
     * Creates a new ProducerService with the specified options.
     *
     * @param produceRegion    the region where messages are produced
     * @param producerProvider function to create producers for storage topics
     * @param topicProvider    provider for topic information
     * @param producerOptions  configuration options for producers
     * @throws NullPointerException if any parameter is null
     */
    public ProducerService(
        String produceRegion,
        Function<StorageTopic, Producer> producerProvider,
        TopicProvider topicProvider,
        ProducerOptions producerOptions
    ) {
        this.produceRegion = Objects.requireNonNull(produceRegion, "Produce region cannot be null");
        Objects.requireNonNull(producerProvider, "Producer provider cannot be null");
        this.topicProvider = Objects.requireNonNull(topicProvider, "Topic provider cannot be null");
        Objects.requireNonNull(producerOptions, "Producer options cannot be null");

        this.producerCache = Caffeine.newBuilder()
                                     .expireAfterAccess(producerOptions.getProducerCacheTtlSeconds(), TimeUnit.SECONDS)
                                     .recordStats()
                                     .build(producerProvider::apply);

        log.debug(
            "Initialized ProducerService with lazy-loading producer cache (TTL: {}s)",
            producerOptions.getProducerCacheTtlSeconds()
        );
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
        Objects.requireNonNull(message, "Message cannot be null");
        Objects.requireNonNull(varadhiTopicName, "Topic name cannot be null");
        Objects.requireNonNull(metricsEmitter, "Metrics emitter cannot be null");

        try {
            return Optional.ofNullable(topicProvider.getEntity(varadhiTopicName))
                           .filter(VaradhiTopic::isActive)
                           .map(topic -> produceToValidTopic(message, topic, metricsEmitter))
                           .orElseThrow(
                               () -> new ResourceNotFoundException(
                                   String.format("Topic(%s) not found or not active.", varadhiTopicName)
                               )
                           );
        } catch (ResourceNotFoundException | ProduceException e) {
            throw e;
        } catch (Exception e) {
            throw new ProduceException("Produce failed due to internal error: " + e.getMessage(), e);
        }
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
        return Optional.ofNullable(varadhiTopic.getProduceTopicForRegion(produceRegion)).map(internalTopic -> {
            if (!internalTopic.getTopicState().isProduceAllowed()) {
                return CompletableFuture.completedFuture(
                    ProduceResult.ofNonProducingTopic(message.getMessageId(), internalTopic.getTopicState())
                );
            }

            StorageTopic storageTopic = internalTopic.getTopicToProduce();
            return getProducer(storageTopic).thenCompose(
                producer -> produceToStorageProducer(producer, metricsEmitter, storageTopic.getName(), message)
                                                                                                               .thenApply(
                                                                                                                   result -> ProduceResult.of(
                                                                                                                       message.getMessageId(),
                                                                                                                       result
                                                                                                                   )
                                                                                                               )
            );
        })
                       .orElseThrow(
                           () -> new ResourceNotFoundException(
                               String.format("Topic not found for region(%s).", produceRegion)
                           )
                       );
    }

    /**
     * Gets a producer for the specified storage topic.
     * <p>
     * First checks if the producer is already in the cache. If not, attempts to load it.
     *
     * @param storageTopic the storage topic to get a producer for
     * @return a future that completes with the producer
     */
    private CompletableFuture<Producer> getProducer(StorageTopic storageTopic) {
        Producer producer = producerCache.getIfPresent(storageTopic);
        if (producer != null) {
            return CompletableFuture.completedFuture(producer);
        }

        try {
            producer = producerCache.get(storageTopic);
            return CompletableFuture.completedFuture(producer);
        } catch (Exception e) {
            String errorMsg = String.format(
                "Error getting producer for Topic(%s): %s",
                storageTopic.getName(),
                e.getMessage()
            );
            log.error(errorMsg, e);
            return CompletableFuture.failedFuture(new ProduceException(errorMsg, e));
        }
    }

    /**
     * Produces a message to a storage topic using the specified producer.
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
}
