package com.flipkart.varadhi.entities;


import com.flipkart.varadhi.AsyncResult;
import com.flipkart.varadhi.exceptions.InvalidStateException;
import lombok.Getter;

@Getter
public class ProduceResult {
    private final String messageId;
    private final ProduceStatus produceStatus;
    private final Throwable throwable;
    private final Offset produceOffset;

    private ProduceResult(
            String messageId, ProduceStatus produceStatus, Offset produceOffset, Throwable throwable
    ) {
        this.messageId = messageId;
        this.produceOffset = produceOffset;
        this.throwable = throwable;
        this.produceStatus = produceStatus;
    }

    public static ProduceResult of(String messageId, AsyncResult<Offset> producerResult) {
        return producerResult.hasResult() ?
                new ProduceResult(messageId, ProduceStatus.Success, producerResult.result(), null) :
                new ProduceResult(messageId, ProduceStatus.Failed, null, producerResult.cause());
    }

    public static ProduceResult ofNonProducingTopic(String messageId, TopicState topicState) {
        if (topicState.isProduceAllowed()) {
            throw new InvalidStateException(
                    String.format(
                            "Incorrect Topic state handling. Topic can produce message(s) in its current state(%s).",
                            topicState
                    ));
        }
        return new ProduceResult(messageId, topicState.getProduceStatus(), null, null);
    }

    public boolean isSuccess() {
        return produceStatus == ProduceStatus.Success;
    }

    public String getFailureReason() {
        String message = produceStatus.getMessage();
        if (throwable != null) {
            message = String.format("%s %s", message, throwable.getMessage());
        }
        return message;
    }
}
