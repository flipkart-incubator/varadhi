package com.flipkart.varadhi.produce.services;

import com.flipkart.varadhi.Result;
import com.flipkart.varadhi.VaradhiCache;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.ProduceException;
import com.flipkart.varadhi.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.exceptions.VaradhiException;
import com.flipkart.varadhi.produce.ProduceResult;
import com.flipkart.varadhi.produce.config.ProducerOptions;
import com.flipkart.varadhi.produce.otel.ProducerMetricsEmitter;
import com.flipkart.varadhi.spi.services.Producer;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;


@Slf4j
public class ProducerService {
    private final VaradhiCache<StorageTopic, Producer> producerCache;
    private final VaradhiCache<String, VaradhiTopic> internalTopicCache;
    private final String produceRegion;

    public ProducerService(
        String produceRegion,
        ProducerOptions producerOptions,
        Function<StorageTopic, Producer> producerProvider,
        Function<String, VaradhiTopic> topicProvider,
        MeterRegistry meterRegistry
    ) {
        this.internalTopicCache = setupTopicCache(
            producerOptions.getTopicCacheBuilderSpec(),
            topicProvider,
            meterRegistry
        );
        this.producerCache = setupProducerCache(
            producerOptions.getProducerCacheBuilderSpec(),
            producerProvider,
            meterRegistry
        );
        this.produceRegion = produceRegion;
    }

    private VaradhiCache<String, VaradhiTopic> setupTopicCache(
        String cacheSpec,
        Function<String, VaradhiTopic> topicProvider,
        MeterRegistry meterRegistry
    ) {
        return new VaradhiCache<>(
            cacheSpec,
            topicProvider,
            (topicName, failure) -> new ProduceException(
                String.format("Failed to get produce Topic(%s). %s", topicName, failure.getMessage()),
                failure
            ),
            "topic",
            meterRegistry
        );
    }


    private VaradhiCache<StorageTopic, Producer> setupProducerCache(
        String cacheSpec,
        Function<StorageTopic, Producer> producerProvider,
        MeterRegistry meterRegistry
    ) {
        return new VaradhiCache<>(
            cacheSpec,
            producerProvider,
            (storageTopic, failure) -> new ProduceException(
                String.format(
                    "Failed to create Pulsar producer for Topic(%s). %s",
                    storageTopic.getName(),
                    failure.getMessage()
                ),
                failure
            ),
            "producer",
            meterRegistry
        );
    }

    public CompletableFuture<ProduceResult> produceToTopic(
        Message message,
        String varadhiTopicName,
        ProducerMetricsEmitter metricsEmitter
    ) {
        try {
            InternalCompositeTopic internalTopic = internalTopicCache.get(varadhiTopicName)
                                                                     .getProduceTopicForRegion(produceRegion);

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
                log.debug(
                    String.format("Produce Message(%s) to StorageTopic(%s) failed.", message.getMessageId(), topic),
                    throwable
                );
            }
            return Result.of(result, throwable);
        });
    }
}
