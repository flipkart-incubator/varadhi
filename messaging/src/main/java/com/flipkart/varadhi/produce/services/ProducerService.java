package com.flipkart.varadhi.produce.services;

import com.flipkart.varadhi.entities.*;
import com.flipkart.varadhi.exceptions.OperationNotAllowedException;
import com.flipkart.varadhi.exceptions.ResourceBlockedException;
import com.flipkart.varadhi.exceptions.ResourceRateLimitedException;
import com.flipkart.varadhi.produce.MsgProduceStatus;
import com.flipkart.varadhi.produce.otel.ProduceMetricProvider;
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
        MsgProduceStatus msgProduceStatus = MsgProduceStatus.Success;
        long produceStartTime = System.currentTimeMillis();
        try {
            String produceRegion = produceContext.getTopicContext().getRegion();
            InternalTopic internalTopic =
                    this.internalTopicCache.getInternalMainTopicForRegion(varadhiTopicName, produceRegion);

            //TODO::Check if below two step process can be done in better way.
            msgProduceStatus = getProduceStatus(internalTopic);
            ensureProduceAllowed(msgProduceStatus);

            addRequestHeadersToMessage(message, produceContext.getRequestContext().getHeaders());
            addVaradhiHeadersToMessage(message, produceContext);
            Producer producer = this.producerCache.getProducer(internalTopic.getStorageTopic());
            CompletableFuture<ProducerResult> producerResult = producer.ProduceAsync(message);
            // TODO::check what happens for thenApply in case of failure.
            return producerResult.thenApply(pResult -> {
                this.metricProvider.onMessageProduceEnd(produceStartTime, MsgProduceStatus.Success, produceContext);
                return new ProduceResult(message, pResult);
            });
        } catch (Exception e) {
            msgProduceStatus = MsgProduceStatus.Success == msgProduceStatus ? MsgProduceStatus.Failed :
                    msgProduceStatus;
            this.metricProvider.onMessageProduceEnd(produceStartTime, msgProduceStatus, produceContext);
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
        if (null != produceContext.getTopicContext()) {
            message.addHeader(
                    HEADER_PRODUCE_REGION,
                    produceContext.getTopicContext().getRegion()
            );
        }
    }

    private void addRequestHeadersToMessage(Message message, Map<String, String> requestHeaders) {
        message.addHeaders(HeaderUtils.getVaradhiHeader(requestHeaders));
    }

    private MsgProduceStatus getProduceStatus(InternalTopic topic) {
        return switch (topic.getTopicStatus()) {
            case Blocked -> MsgProduceStatus.Blocked;
            case Throttled -> MsgProduceStatus.Throttled;
            case NotAllowed -> MsgProduceStatus.NotAllowed;
            default -> MsgProduceStatus.Success;
        };
    }

    private void ensureProduceAllowed(MsgProduceStatus status) {
        switch (status) {
            case Blocked -> throw new ResourceBlockedException("Topic is currently blocked for produce.");
            case Throttled -> throw new ResourceRateLimitedException("Topic is being throttled. Try again.");
            case NotAllowed -> throw new OperationNotAllowedException("Produce is not allowed for the topic.");
        }
    }

}
