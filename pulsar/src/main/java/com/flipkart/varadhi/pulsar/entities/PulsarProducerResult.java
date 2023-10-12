package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.entities.ProducerResult;
import com.flipkart.varadhi.exceptions.ArgumentException;
import org.apache.pulsar.client.api.MessageId;

public class PulsarProducerResult implements ProducerResult {
    private final MessageId messageId;

    public PulsarProducerResult(MessageId messageId) {
        this.messageId = messageId;
    }

    @Override
    public int compareTo(ProducerResult o) {
        if (null == o) {
            throw new ArgumentException("Can not compare null ProducerResult.");
        }
        if (o instanceof PulsarProducerResult) {
            return messageId.compareTo(((PulsarProducerResult) o).messageId);
        } else {
            throw new ArgumentException(String.format(
                    "Can not compare different ProducerResult types. Expected ProducerResult is %s, given  %s.",
                    PulsarProducerResult.class.getName(), o.getClass().getName()
            ));
        }
    }

    public String toString() {
        return messageId.toString();
    }
}
