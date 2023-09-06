package com.flipkart.varadhi.produce.services;

import com.flipkart.varadhi.AsyncResult;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.ProduceException;
import com.flipkart.varadhi.exceptions.VaradhiException;
import com.flipkart.varadhi.produce.otel.ProducerMetricProvider;
import com.flipkart.varadhi.spi.services.Producer;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class ProducerService {
    private final ProducerCache producerCache;
    private final InternalTopicCache internalTopicCache;
    private final ProducerMetricProvider metricProvider;

    public ProducerService(
            ProducerCache producerCache, InternalTopicCache topicCache, ProducerMetricProvider metricProvider
    ) {
        this.producerCache = producerCache;
        this.internalTopicCache = topicCache;
        this.metricProvider = metricProvider;
    }

    public CompletableFuture<ProduceResult> produceToTopic(
            Message message,
            String varadhiTopicName,
            ProduceContext context
    ) {
        try {
            String produceRegion = context.getTopicContext().getRegion();
            InternalTopic internalTopic = internalTopicCache.getProduceTopicForRegion(varadhiTopicName, produceRegion);
            if (!internalTopic.getTopicState().isProduceAllowed()) {
                return CompletableFuture.completedFuture(
                        ProduceResult.ofNonProducingTopic(message.getMessageId(), internalTopic.getTopicState()));
            }
            Producer producer = producerCache.getProducer(internalTopic.getStorageTopic());
            return produceToStorageProducer(
                    producer, context, internalTopic.getStorageTopic().getName(), message).thenApply(result ->
                    ProduceResult.of(message.getMessageId(), result));
        } catch (VaradhiException e) {
            throw e;
        } catch (Exception e) {
            throw new ProduceException("Produce failed due to internal error.", e);
        }
    }


    private CompletableFuture<AsyncResult<ProducerResult>> produceToStorageProducer(
            Producer producer, ProduceContext context, String topic, Message message
    ) {
        long produceStart = System.currentTimeMillis();
        return producer.ProduceAsync(message).handle((result, throwable) -> {
            int producerLatency = (int) (System.currentTimeMillis() - produceStart);
            emitProducerMetric(message.getMessageId(), result != null, producerLatency, context);
            if (throwable != null) {
                log.debug(
                        String.format("Produce Message(%s) to StorageTopic(%s) failed.", message.getMessageId(), topic),
                        throwable
                );
            }
            return AsyncResult.of(result, throwable);
        });
    }

    private void emitProducerMetric(String messageId, boolean succeeded, int produceLatency, ProduceContext context) {
        try {
            metricProvider.OnMessageProduced(succeeded, produceLatency, context);
        } catch (Exception e) {
            // Failure in metric path, shouldn't fail the metric. Log and ignore any exception.
            ProduceContext.TopicContext tContext = context.getTopicContext();
            String message = String.format("Failed to send metric: %s:%s:%s(%s) Succeeded(%b) Latency(%d).",
                    tContext.getRegion(), tContext.getProjectName(),
                    tContext.getTopicName(), messageId, succeeded, produceLatency
            );
            log.error("{}: {}", message, e.getMessage());
        }
    }
}
