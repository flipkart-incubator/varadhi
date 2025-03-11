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
import com.flipkart.varadhi.produce.ProducerErrorMapper;
import com.flipkart.varadhi.produce.config.ProducerErrorType;
import com.flipkart.varadhi.produce.config.ProducerOptions;
import com.flipkart.varadhi.produce.otel.ProducerMetricsEmitter;
import com.flipkart.varadhi.pulsar.entities.PulsarOffset;
import com.flipkart.varadhi.spi.services.Producer;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Service responsible for managing message production to topics in the Varadhi messaging system.
 * Provides caching of producers and topics, and handles message production with metrics tracking.
 *
 * <p>This service implements thread-safe message production with the following features:
 * <ul>
 *   <li>Producer and topic caching for improved performance</li>
 *   <li>Metrics collection for monitoring and alerting</li>
 *   <li>Asynchronous message production with CompletableFuture</li>
 *   <li>Comprehensive error handling and reporting</li>
 * </ul>
 */
@Slf4j
public class ProducerService {
    private final VaradhiCache<StorageTopic, Producer> producerCache;
    private final VaradhiCache<String, VaradhiTopic> internalTopicCache;
    private final String produceRegion;

    /**
     * Constructs a new ProducerService with the specified configuration and dependencies.
     *
     * @param produceRegion    The region identifier where messages will be produced
     * @param producerOptions  Configuration options for producer behavior
     * @param producerProvider Factory function for creating new producers
     * @param topicProvider    Function to retrieve topic information
     * @param meterRegistry    Registry for metrics collection
     */
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

    /**
     * Sets up the topic cache with specified configuration.
     */
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

    /**
     * Sets up the producer cache with specified configuration.
     */
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

    /**
     * Produces a message to the specified topic asynchronously.
     *
     * @param message          The message to produce
     * @param varadhiTopicName The fully qualified topic name
     * @param metricsEmitter   Emitter for tracking production metrics
     * @return A future containing the result of the produce operation
     * @throws ResourceNotFoundException if the topic is not found
     * @throws ProduceException          if there's an error during production
     */
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
                metricsEmitter.emit(false, 0, 0, 0,
                        false, ProducerErrorType.TOPIC_NOT_FOUND);
                throw new ResourceNotFoundException(String.format("Topic not found for region(%s).", produceRegion));
            }

            if (!internalTopic.getTopicState().isProduceAllowed()) {
                metricsEmitter.emit(false, 0, 0, 0,
                        true, ProducerErrorMapper.mapTopicStateToErrorType(internalTopic.getTopicState()));
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
            metricsEmitter.emit(false, 0, 0, 0,
                    false, ProducerErrorMapper.mapToProducerErrorType(e));
            throw e;
        } catch (Exception e) {
            metricsEmitter.emit(false, 0, 0, 0,
                    false, ProducerErrorType.INTERNAL);
            throw new ProduceException(String.format("Produce failed due to internal error: %s", e.getMessage()), e);
        }
    }

    /**
     * Asynchronously produces a message to the storage layer through the specified producer.
     * Handles metrics emission and error logging for the produce operation.
     *
     * @param producer       The storage producer instance to use
     * @param metricsEmitter The metrics emitter for tracking production metrics
     * @param topic          The name of the topic being produced to
     * @param message        The message to be produced
     * @return A CompletableFuture containing the Result with either an Offset or an error
     */
    private CompletableFuture<Result<Offset>> produceToStorageProducer(
        Producer producer,
        ProducerMetricsEmitter metricsEmitter,
        String topic,
        Message message
    ) {
        return producer.produceAsync(message).handle((result, throwable) -> {
            long storageLatency = result != null ? ((PulsarOffset)result).getStorageLatencyMs() : 0;
            metricsEmitter.emit(
                    result != null,
                    0,
                    storageLatency,
                    message.getPayload().length,
                    false,
                    throwable != null ? ProducerErrorMapper.mapToProducerErrorType(throwable) : null
            );
            if (throwable != null) {
                log.debug("Produce Message({}) to StorageTopic({}) failed.", message.getMessageId(), topic, throwable);
            }
            return Result.of(result, throwable);
        });
    }
}
