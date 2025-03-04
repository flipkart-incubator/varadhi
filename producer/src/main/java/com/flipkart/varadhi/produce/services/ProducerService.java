package com.flipkart.varadhi.produce.services;

import com.flipkart.varadhi.Result;
import com.flipkart.varadhi.VaradhiCache;
import com.flipkart.varadhi.entities.InternalCompositeTopic;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.exceptions.ProduceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.exceptions.VaradhiException;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.produce.otel.ProducerMetricsEmitter;
import com.flipkart.varadhi.spi.services.Producer;
import io.micrometer.core.instrument.MeterRegistry;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

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

    public ProducerService(
        String produceRegion,
        Function<StorageTopic, Producer> producerProvider,
        MeterRegistry meterRegistry
    ) {
        this.produceRegion = Objects.requireNonNull(produceRegion, "Produce region cannot be null");
        this.producerProvider = Objects.requireNonNull(producerProvider, "Producer provider cannot be null");
        Objects.requireNonNull(meterRegistry, "Meter registry cannot be null");

        this.internalTopicCache = new VaradhiCache<>(TOPIC_CACHE_NAME, meterRegistry);
        this.producerCache = new VaradhiCache<>(PRODUCER_CACHE_NAME, meterRegistry);
    }

    public CompletableFuture<ProduceResult> produceToTopic(
        Message message,
        String varadhiTopicName,
        ProducerMetricsEmitter metricsEmitter
    ) {
        try {
            VaradhiTopic varadhiTopic = internalTopicCache.get(varadhiTopicName);
            if (varadhiTopic == null || !varadhiTopic.isActive()) {
                throw new ResourceNotFoundException(String.format("Topic(%s) not found or is inactive.", varadhiTopicName));
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
            Producer producer = producerCache.get(internalTopic.getTopicToProduce());
            return produceToStorageProducer(
                producer,
                metricsEmitter,
                internalTopic.getTopicToProduce().getName(),
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

    public Future<Void> initializeCaches(List<VaradhiTopic> topics) {
        Promise<Void> promise = Promise.promise();
        try {
            log.info("Starting cache initialization with {} topics", topics.size());

            initializeTopicCache(topics);

            initializeProducerCache(topics);

            log.info("Successfully initialized all caches");
            promise.complete();
        } catch (Exception e) {
            log.error("Failed to initialize caches", e);
            promise.fail(e);
        }
        return promise.future();
    }

    private void initializeTopicCache(List<VaradhiTopic> topics) {
        log.info("Initializing topic cache");
        topics.forEach(topic -> internalTopicCache.put(topic.getName(), topic));
        log.info("Topic cache initialized with {} entries", topics.size());
    }

    private void initializeProducerCache(List<VaradhiTopic> topics) {
        var activeStorageTopics = topics.stream()
                .map(topic -> topic.getProduceTopicForRegion(produceRegion))
                .filter(Objects::nonNull)
                .filter(internalTopic -> internalTopic.getTopicState().isProduceAllowed())
                .map(InternalCompositeTopic::getTopicToProduce)
                .toList();

        log.info("Initializing producer cache with {} active topics", activeStorageTopics.size());

        activeStorageTopics.forEach(storageTopic -> {
            try {
                Producer producer = producerProvider.apply(storageTopic);
                producerCache.put(storageTopic, producer);
            } catch (Exception e) {
                log.error("Failed to initialize producer for topic: {}", storageTopic.getName(), e);
                throw e;
            }
        });

        log.info("Producer cache initialized successfully");
    }
}
