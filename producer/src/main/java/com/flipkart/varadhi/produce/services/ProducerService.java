package com.flipkart.varadhi.produce.services;

import com.flipkart.varadhi.common.Result;
import com.flipkart.varadhi.common.VaradhiCache;
import com.flipkart.varadhi.common.exceptions.ProduceException;
import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.common.exceptions.VaradhiException;
import com.flipkart.varadhi.entities.InternalCompositeTopic;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.produce.otel.ProducerMetricsEmitter;
import com.flipkart.varadhi.spi.db.MetaStore;
import com.flipkart.varadhi.spi.services.Producer;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Service for managing producers and topic caches in Varadhi.
 * <p>
 * This service provides methods for producing messages to topics and managing caches
 * of topics and producers for efficient access.
 */
@Slf4j
public class ProducerService {
    private static final String TOPIC_CACHE_NAME = "topic";
    private static final String PRODUCER_CACHE_NAME = "producer";

    @Getter
    private final VaradhiCache<StorageTopic, Producer> producerCache;

    @Getter
    private final VaradhiCache<String, VaradhiTopic> internalTopicCache;

    private final String produceRegion;
    private final Function<StorageTopic, Producer> producerProvider;
    private final MetaStore metaStore;

    /**
     * Creates a new ProducerService with the specified parameters.
     *
     * @param produceRegion    the region for producing messages
     * @param producerProvider the function to create producers for storage topics
     * @param meterRegistry    the registry for metrics
     * @param metaStore        the MetaStore for persistent storage
     * @throws NullPointerException if any parameter is null
     */
    public ProducerService(
        String produceRegion,
        Function<StorageTopic, Producer> producerProvider,
        MeterRegistry meterRegistry,
        MetaStore metaStore
    ) {
        this.produceRegion = Objects.requireNonNull(produceRegion, "Produce region cannot be null");
        this.producerProvider = Objects.requireNonNull(producerProvider, "Producer provider cannot be null");
        Objects.requireNonNull(meterRegistry, "Meter registry cannot be null");
        this.metaStore = Objects.requireNonNull(metaStore, "MetaStore cannot be null");

        this.internalTopicCache = new VaradhiCache<>(TOPIC_CACHE_NAME, meterRegistry);
        this.producerCache = new VaradhiCache<>(PRODUCER_CACHE_NAME, meterRegistry);
    }

    /**
     * Preloads all topics and their producers into the cache.
     * <p>
     * This method should be called during system startup to ensure the cache is populated
     * before handling requests.
     *
     * @return a Future that completes when the cache is preloaded
     */
    public Future<Void> preloadCache() {
        Promise<Void> promise = Promise.promise();
        try {
            log.info("Starting to preload topic and producer caches");
            List<VaradhiTopic> topics = metaStore.getAllTopics();

            if (topics.isEmpty()) {
                log.info("No topics found to preload");
                promise.complete();
                return promise.future();
            }

            // Build topic cache entries
            Map<String, VaradhiTopic> topicMap = topics.stream()
                    .collect(Collectors.toMap(VaradhiTopic::getName, Function.identity()));

            // Build producer cache entries
            Map<StorageTopic, Producer> producerMap = new HashMap<>();
            for (VaradhiTopic topic : topics) {
                InternalCompositeTopic internalTopic = topic.getProduceTopicForRegion(produceRegion);
                if (internalTopic != null && internalTopic.getTopicState().isProduceAllowed()) {
                    StorageTopic storageTopic = internalTopic.getTopicToProduce();
                    Producer producer = producerProvider.apply(storageTopic);
                    producerMap.put(storageTopic, producer);
                }
            }

            // Populate caches
            internalTopicCache.putAll(topicMap);
            producerCache.putAll(producerMap);

            log.info("Successfully preloaded {} topics and {} producers into cache",
                    topicMap.size(), producerMap.size());
            promise.complete();
        } catch (Exception e) {
            log.error("Failed to preload topic and producer caches", e);
            promise.fail(e);
        }
        return promise.future();
    }

    /**
     * Produces a message to a topic.
     *
     * @param message          the message to produce
     * @param varadhiTopicName the name of the topic to produce to
     * @param metricsEmitter   the emitter for metrics
     * @return a CompletableFuture that completes with the result of the produce operation
     * @throws ResourceNotFoundException if the topic does not exist or is inactive
     * @throws ProduceException          if there is an error producing the message
     */
    public CompletableFuture<ProduceResult> produceToTopic(
        Message message,
        String varadhiTopicName,
        ProducerMetricsEmitter metricsEmitter
    ) {
        try {
            VaradhiTopic varadhiTopic = internalTopicCache.getOrCompute(varadhiTopicName, name -> {
                try {
                    return metaStore.getTopic(name);
                } catch (ResourceNotFoundException e) {
                    log.warn("Topic not found in MetaStore: {}", name);
                    throw e;
                }
            });

            if (!varadhiTopic.isActive()) {
                throw new ResourceNotFoundException(
                        String.format("Topic(%s) is inactive.", varadhiTopicName)
                );
            }

            InternalCompositeTopic internalTopic = varadhiTopic.getProduceTopicForRegion(produceRegion);

            // TODO: evaluate, if there is no reason for this to be null. It should IllegalStateException if it is null.
            if (internalTopic == null) {
                throw new ResourceNotFoundException(String.format("Topic not found for region(%s).", produceRegion));
            }

            if (!internalTopic.getTopicState().isProduceAllowed()) {
                return CompletableFuture.completedFuture(
                    ProduceResult.ofNonProducingTopic(message.getMessageId(), internalTopic.getTopicState())
                );
            }

            StorageTopic storageTopic = internalTopic.getTopicToProduce();
            Producer producer = producerCache.getOrCompute(storageTopic, this.producerProvider);

            return produceToStorageProducer(
                producer,
                metricsEmitter,
                storageTopic.getName(),
                message
            ).thenApply(result -> ProduceResult.of(message.getMessageId(), result));
        } catch (VaradhiException e) {
            throw e;
        } catch (Exception e) {
            throw new ProduceException(String.format("Produce failed due to internal error: %s", e.getMessage()), e);
        }
    }

    private CompletableFuture<Result<Offset>> produceToStorageProducer(
        Producer producer,
        ProducerMetricsEmitter metricsEmitter,
        String topic,
        Message message
    ) {
        long produceStart = System.currentTimeMillis();
        return producer.produceAsync(message).handle((result, throwable) -> {
            int producerLatency = (int)(System.currentTimeMillis() - produceStart);
            metricsEmitter.emit(result != null, producerLatency);
            if (throwable != null) {
                log.debug("Produce Message({}) to StorageTopic({}) failed.", message.getMessageId(), topic, throwable);
            }
            return Result.of(result, throwable);
        });
    }
}
