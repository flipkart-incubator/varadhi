package com.flipkart.varadhi.produce;

import com.flipkart.varadhi.common.Result;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.ProduceStatus;
import com.flipkart.varadhi.entities.TopicState;
import lombok.Getter;

/**
 * Represents the result of a message production operation in the Varadhi messaging system.
 * This class encapsulates all relevant information about the outcome of a produce operation,
 * including success/failure status, message ID, and any error information.
 */
@Getter
public class ProduceResult {
    private final String messageId;
    private final ProduceStatus produceStatus;
    private final Throwable throwable;
    private final Offset produceOffset;

    // not final, as these info can be set later on.
    private long latencyMs;
    private int msgSizeBytes;

    private ProduceResult(String messageId, ProduceStatus produceStatus, Offset produceOffset, Throwable throwable) {
        this.messageId = messageId;
        this.produceOffset = produceOffset;
        this.throwable = throwable;
        this.produceStatus = produceStatus;
    }

    /**
     * Creates a ProduceResult instance from a message ID and producer result.
     *
     * @param messageId      the unique identifier of the message
     * @param producerResult the result of the produce operation
     * @return a new ProduceResult instance
     */
    public static ProduceResult of(String messageId, Result<Offset> producerResult) {
        return producerResult.hasResult() ?
            new ProduceResult(messageId, ProduceStatus.Success, producerResult.result(), null) :
            new ProduceResult(messageId, ProduceStatus.Failed, null, producerResult.cause());
    }

    /**
     * Creates a ProduceResult instance for a non-producing topic.
     *
     * @param messageId  the unique identifier of the message
     * @param topicState the current state of the topic
     * @return a new ProduceResult instance
     * @throws IllegalStateException if the topic state allows message production
     */
    public static ProduceResult ofNonProducingTopic(String messageId, TopicState topicState) {
        if (topicState.isProduceAllowed()) {
            throw new IllegalStateException(
                "Incorrect Topic state handling. Topic can produce message(s) in its current state(%s).".formatted(
                    topicState
                )
            );
        }
        return new ProduceResult(messageId, topicState.getProduceStatus(), null, null);
    }

    public static ProduceResult ofFilteredMessage(String messageId) {
        return new ProduceResult(messageId, ProduceStatus.Filtered, null, null);
    }

    /**
     * Checks if the produce operation was successful.
     *
     * @return true if the operation succeeded, false otherwise
     */
    public boolean isSuccess() {
        return produceStatus == ProduceStatus.Success;
    }

    /**
     * Gets a detailed failure reason if the produce operation failed.
     * Combines the produce status message with any available exception message.
     *
     * @return a string describing the failure reason
     */
    public String getFailureReason() {
        String message = produceStatus.getMessage();
        if (throwable != null) {
            Throwable rootCause = throwable.getCause() == null ? throwable : throwable.getCause();
            message = "%s %s".formatted(message, rootCause.getMessage());
        }
        return message;
    }
}
