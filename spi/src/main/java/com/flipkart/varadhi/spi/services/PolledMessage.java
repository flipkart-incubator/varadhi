package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;

/**
 * Represents a single message polled from some partition.
 */
// TODO: Keeping payload as byte[] only for now. When implementing, there will be opportunity to evaluate how to avoid
// unnecessary deserialization / array copy & then we can tweak this interface.
public interface PolledMessage<O extends Offset> extends Message {

    long getProducedTimestampMs();

    String getTopicName();

    /**
     * @return partition info if available. -1 otherwise.
     */
    int getPartition();

    /**
     * @return the offset of this message in the partition.
     */
    O getOffset();

    /**
     * @return the payload of this message.
     */
    // TODO: evaluate ByteBuf instead of byte[]. byte[] means heap allocated.
    byte[] getPayload();

    // TODO: evaluate method for message properties that live outside of payload.

    /**
     * releases any resources that may be associated to it. Accessing message object once released is undefined.
     */
    void release();
}
