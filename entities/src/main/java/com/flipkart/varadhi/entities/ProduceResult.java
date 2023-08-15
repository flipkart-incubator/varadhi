package com.flipkart.varadhi.entities;


import com.flipkart.varadhi.exceptions.InvalidStateException;
import lombok.Getter;

import java.util.Optional;

@Getter
public class ProduceResult {

    private final String messageId;
    private final ProduceStatus produceStatus;
    private final Optional<ProducerResult> producerResult;
    private final long producerLatency;

    private ProduceResult(
            String messageId, ProducerResult producerResult, ProduceStatus produceStatus, long producerLatency
    ) {
        this.messageId = messageId;
        this.producerResult = Optional.of(producerResult);
        this.producerLatency = producerLatency;
        this.produceStatus = produceStatus;
    }

    private ProduceResult(String messageId, ProduceStatus produceStatus, long producerLatency) {
        this.messageId = messageId;
        this.producerResult = Optional.empty();
        this.producerLatency = producerLatency;
        this.produceStatus = produceStatus;
    }

    public static ProduceResult onSuccess(String messageId, ProducerResult producerResult, long producerLatency) {
        return new ProduceResult(
                messageId, producerResult, new ProduceStatus(Status.Success, "Completed"), producerLatency);
    }

    public static ProduceResult onProducerFailure(String messageId, long producerLatency, String errorMessage) {
        return new ProduceResult(
                messageId,
                new ProduceStatus(
                        Status.Failed,
                        String.format("Produce failed at messaging stack: %s.", errorMessage)
                ),
                producerLatency
        );
    }

    public static ProduceResult onNonProducingTopicState(String messageId, InternalTopic.TopicState topicState) {
        return new ProduceResult(messageId, getNonProducingState(topicState), 0);
    }

    private static ProduceStatus getNonProducingState(InternalTopic.TopicState topicState) {
        return switch (topicState) {
            case Blocked -> new ProduceStatus(
                    Status.Blocked,
                    "Topic is blocked. Unblock the topic before produce."
            );
            case Throttled -> new ProduceStatus(
                    Status.Throttled,
                    "Produce to Topic is currently rate limited, try again after sometime."
            );
            case Replicating -> new ProduceStatus(
                    Status.NotAllowed,
                    "Produce is not allowed for replicating topic."
            );
            default -> throw new InvalidStateException(
                    String.format(
                            "Incorrect Topic state handling. Topic can produce message(s) in its current state(%s).",
                            topicState
                    ));
        };
    }

    public boolean isSuccess() {
        return this.getProduceStatus().status == Status.Success;
    }

    @Getter
    public enum Status {
        Failed("Failed"),
        Throttled("Throttled"),
        Blocked("Blocked"),
        NotAllowed("NotAllowed"),
        Success("Succeeded");

        private final String tagCategory;

        Status(String tagCategory) {
            this.tagCategory = tagCategory;
        }
    }

    public record ProduceStatus(Status status, String message) {
    }

}
