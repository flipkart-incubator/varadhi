package com.flipkart.varadhi.produce.services;

import com.flipkart.varadhi.VaradhiCache;
import com.flipkart.varadhi.core.VaradhiTopicService;
import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.ProduceException;
import com.flipkart.varadhi.exceptions.VaradhiException;
import com.flipkart.varadhi.produce.config.ProducerOptions;
import com.flipkart.varadhi.produce.otel.ProduceMetricProvider;
import com.flipkart.varadhi.spi.services.Producer;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.flipkart.varadhi.entities.ProduceResult.Status.Failed;

@Slf4j
public class ProducerService {
    private final VaradhiCache<StorageTopic, Producer> producerCache;
    private final VaradhiCache<String, VaradhiTopic> internalTopicCache;
    private final ProduceMetricProvider metricProvider;


    public ProducerService(
            ProducerOptions producerOptions,
            VaradhiTopicService varadhiTopicService,
            ProducerFactory<StorageTopic> producerFactory,
            MeterRegistry meterRegistry
    ) {
        this.internalTopicCache =
                setupTopicCache(producerOptions.getTopicCacheBuilderSpec(), varadhiTopicService::get, meterRegistry);
        this.producerCache =
                setupProducerCache(producerOptions.getProducerCacheBuilderSpec(), producerFactory::getProducer,
                        meterRegistry
                );
        this.metricProvider = new ProduceMetricProvider(meterRegistry);
    }

    private VaradhiCache<String, VaradhiTopic> setupTopicCache(
            String cacheSpec, Function<String, VaradhiTopic> topicProvider, MeterRegistry meterRegistry
    ) {
        return new VaradhiCache<>(
                cacheSpec,
                topicProvider,
                (topicName, failure) -> new ProduceException(
                        String.format("Failed to get produce Topic(%s). %s", topicName, failure.getMessage()), failure),
                "topic",
                meterRegistry
        );
    }


    private VaradhiCache<StorageTopic, Producer> setupProducerCache(
            String cacheSpec, Function<StorageTopic, Producer> producerProvider, MeterRegistry meterRegistry
    ) {
        return new VaradhiCache<>(
                cacheSpec,
                producerProvider,
                (storageTopic, failure) -> new ProduceException(
                        String.format(
                                "Failed to create Pulsar producer for Topic(%s). %s", storageTopic.getName(),
                                failure.getMessage()
                        ), failure),
                "topic",
                meterRegistry
        );
    }

    //
    // TODO:: Discuss impact of bringing validations (topic exists, rate limit, blocked) at this layer.
    // Rest layer will still be processing & doing basic validations before message produce can be rejected from here.

    public CompletableFuture<ProduceResult> produceToTopic(
            Message message,
            String varadhiTopicName,
            ProduceContext context
    ) {
        String messageId = message.getMessageId();
        try {
            String produceRegion = context.getTopicContext().getRegion();
            InternalTopic internalTopic =
                    internalTopicCache.get(varadhiTopicName).getProduceTopicForRegion(produceRegion);

            return produceToTopic(message, internalTopic).thenApply(result -> {
                sendProduceMetric(messageId, result.getProduceStatus().status(), result.getProducerLatency(), context);
                return result;
            });
        } catch (VaradhiException e) {
            sendProduceMetric(messageId, Failed, 0, context);
            throw e;
        } catch (Exception e) {
            sendProduceMetric(messageId, Failed, 0, context);
            throw new ProduceException(String.format("Produce failed due to internal error: %s", e.getMessage()), e);
        }
    }

    private void sendProduceMetric(
            String messageId,
            ProduceResult.Status status,
            int produceLatency,
            ProduceContext context
    ) {
        try {
            metricProvider.OnProduceEnd(messageId, status, produceLatency, context);
        } catch (Exception e) {
            // Failure in metric path, shouldn't fail the metric. Log and ignore any exception.
            log.error(
                    "Failed to send metrics for Produce({}) for {}.{}. Error: {}",
                    messageId,
                    context.getTopicContext().getProject(),
                    context.getTopicContext().getTopic(),
                    e.getMessage()
            );
        }
    }

    private CompletableFuture<ProduceResult> produceToTopic(Message message, InternalTopic internalTopic)
            throws Exception {
        String messageId = message.getMessageId();
        if (internalTopic.produceAllowed()) {
            long produceStart = System.currentTimeMillis();
            Producer producer = producerCache.get(internalTopic.getStorageTopic());

            return producer.ProduceAsync(message).handle((result, throwable) -> {
                int producerLatency = (int) (System.currentTimeMillis() - produceStart);
                if (throwable != null) {
                    log.error(String.format("Produce Message(%s) to StorageTopic(%s) failed.", messageId,
                            internalTopic.getStorageTopic().getName()
                    ), throwable);
                    return ProduceResult.onProducerFailure(messageId, producerLatency, throwable.getMessage());
                } else {
                    return ProduceResult.onSuccess(messageId, result, producerLatency);
                }
            });
        }
        return CompletableFuture.completedFuture(
                ProduceResult.onNonProducingTopicState(messageId, internalTopic.getTopicState()));
    }
}
