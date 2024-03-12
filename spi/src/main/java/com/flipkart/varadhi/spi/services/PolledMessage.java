package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Offset;

/**
 * Represents a single message polled from some partition.
 */
// TODO: Keeping payload as byte[] only for now. When implementing, there will be opportunity to evaluate how to avoid
// unnecessary deserialization / array copy & then we can tweak this interface.
public interface PolledMessage<O extends Offset> {

    /**
     * @return the offset of this message in the partition.
     */
    O offset();

    /**
     * @return the payload of this message.
     */
    byte[] payload();

    // TODO: evaluate method for message properties that live outside of payload.
}
