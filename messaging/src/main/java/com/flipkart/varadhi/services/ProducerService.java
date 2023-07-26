package com.flipkart.varadhi.services;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.OperationNotAllowedException;
import com.flipkart.varadhi.exceptions.ResourceBlockedException;
import com.flipkart.varadhi.exceptions.ResourceRateLimitedException;
import com.flipkart.varadhi.otel.ProduceMetricProvider;
import com.flipkart.varadhi.utils.HeaderUtils;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.flipkart.varadhi.MessageConstants.*;

public class ProducerService {
    private final ProducerCache producerCache;
    private final InternalTopicCache internalTopicCache;
    private final ProduceMetricProvider metricProvider;

    public ProducerService(
            ProducerCache producerCache, InternalTopicCache topicCache, ProduceMetricProvider metricProvider
    ) {
        this.producerCache = producerCache;
        this.internalTopicCache = topicCache;
        this.metricProvider = metricProvider;
    }

    public CompletableFuture<ProduceResult> produceToTopic(
            Message message, String varadhiTopicName, ProduceContext produceContext
    ) {
        long produceStart = System.currentTimeMillis();
        try {
            addRequestHeadersToMessage(message, produceContext.getRequestContext().getHeaders());
            addVaradhiHeadersToMessage(message, produceContext);
            String produceRegion = produceContext.getClusterContext().getProduceRegion();
            InternalTopic internalTopic =
                    this.internalTopicCache.getInternalMainTopicForRegion(varadhiTopicName, produceRegion);
            ensureProduceAllowedForTopic(internalTopic);
            Producer producer = this.producerCache.getProducer(internalTopic.getStorageTopic());
            CompletableFuture<ProducerResult> producerResult = producer.ProduceAsync(message);
            // TODO::check what happens for thenApply in case of failure.
            return producerResult.thenApply(pResult -> {
                // TODO:: add possible tags to metric.
                this.metricProvider.onProduceCompleted(produceStart, System.currentTimeMillis(), produceContext, null);
                return new ProduceResult(message, pResult);
            });
        } catch (Exception e) {
            // TODO::Handle passing failure info as well for further categorisation.
            this.metricProvider.onProduceFailed(produceStart, System.currentTimeMillis(), produceContext, null);
            throw e;
        }
    }

    private void addVaradhiHeadersToMessage(Message message, ProduceContext produceContext) {
        if (null != produceContext.getRequestContext()) {
            message.addHeader(
                    HEADER_PRODUCE_TIMESTAMP,
                    Long.toString(produceContext.getRequestContext().getRequestTimestamp())
            );
        }
        if (null != produceContext.getUserContext()) {
            message.addHeader(
                    HEADER_PRODUCE_IDENTITY,
                    produceContext.getUserContext().getSubject()
            );
        }
        if (null != produceContext.getClusterContext()) {
            message.addHeader(
                    HEADER_PRODUCE_REGION,
                    produceContext.getClusterContext().getProduceRegion()
            );
        }
    }


    private void addRequestHeadersToMessage(Message message, Map<String, String> requestHeaders) {
        message.addHeaders(HeaderUtils.getVaradhiHeader(requestHeaders));
    }

    private void ensureProduceAllowedForTopic(InternalTopic topic) {
        switch (topic.getStatus()) {
            case Blocked -> throw new ResourceBlockedException("Topic is currently blocked for produce.");
            case Throttled -> throw new ResourceRateLimitedException("Topic is being throttled. Try again.");
            case NotAllowed -> throw new OperationNotAllowedException("Produce is not allowed for the topic.");
        }
    }
}
