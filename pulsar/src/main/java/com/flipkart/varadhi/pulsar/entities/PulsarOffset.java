package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.entities.Offset;
import org.apache.pulsar.client.api.MessageId;

public class PulsarOffset implements Offset {
    private final MessageId messageId;

    public PulsarOffset(MessageId messageId) {
        this.messageId = messageId;
    }

    @Override
    public int compareTo(Offset o) {
        if (null == o) {
            throw new IllegalArgumentException("Can not compare null Offset.");
        }
        if (o instanceof PulsarOffset) {
            return messageId.compareTo(((PulsarOffset) o).messageId);
        } else {
            throw new IllegalArgumentException(String.format(
                    "Can not compare different Offset types. Expected Offset is %s, given  %s.",
                    PulsarOffset.class.getName(), o.getClass().getName()
            ));
        }
    }

    @Override
    public String toString() {
        return messageId.toString();
    }
}
