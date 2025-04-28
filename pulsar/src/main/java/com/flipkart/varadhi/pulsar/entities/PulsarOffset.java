package com.flipkart.varadhi.pulsar.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.pulsar.util.MessageIdDeserializer;
import com.flipkart.varadhi.pulsar.util.MessageIdSerializer;
import lombok.Getter;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;

import java.util.Objects;

/**
 * Implementation of {@link Offset} for Apache Pulsar messaging system.
 * Represents a position in a Pulsar topic using Pulsar's {@link MessageId}.
 * Provides functionality for serialization/deserialization and comparison of offsets.
 *
 * @see Offset
 * @see MessageId
 */
@Getter
public final class PulsarOffset implements Offset {

    @JsonSerialize (using = MessageIdSerializer.class)
    @JsonDeserialize (using = MessageIdDeserializer.class)
    private final MessageId messageId;

    private long storageLatencyMs;

    /**
     * Default constructor for Jackson deserialization.
     */
    public PulsarOffset() {
        this.messageId = null;
        this.storageLatencyMs = 0;
    }

    /**
     * Constructs a PulsarOffset with the specified MessageId.
     *
     * @param messageId the Pulsar message identifier
     */
    public PulsarOffset(MessageId messageId) {
        this.messageId = messageId;
    }

    /**
     * Constructs a PulsarOffset with the specified MessageId and storage latency.
     *
     * @param messageId        the Pulsar message identifier
     * @param storageLatencyMs the storage latency in milliseconds
     */
    public PulsarOffset(MessageId messageId, long storageLatencyMs) {
        this.messageId = messageId;
        this.storageLatencyMs = storageLatencyMs;
    }

    /**
     * Creates a PulsarOffset from a string representation.
     *
     * @param offset the string representation of the offset
     * @return a new PulsarOffset instance
     * @throws IllegalArgumentException if the offset format is invalid
     */
    public static PulsarOffset of(String offset) {
        MessageId mId = messageIdFrom(offset);
        return new PulsarOffset(mId);
    }

    /**
     * Creates a MessageId from its component parts.
     *
     * @param ledgerId    the ledger ID
     * @param entryId     the entry ID
     * @param partitionId the partition ID
     * @return a new MessageId instance
     * @throws NumberFormatException if any of the string parameters cannot be parsed to numbers
     */
    public static MessageId fromParts(String ledgerId, String entryId, String partitionId) {
        return new MessageIdImpl(Long.parseLong(ledgerId), Long.parseLong(entryId), Integer.parseInt(partitionId));
    }

    /**
     * Parses a string representation of a MessageId.
     *
     * @param offset the string representation in format "mId:ledgerId:entryId:partitionId"
     * @return a new MessageId instance
     * @throws IllegalArgumentException if the offset format is invalid
     */
    public static MessageId messageIdFrom(String offset) {
        String[] parts = offset.split(":");
        if ("mId".equals(parts[0]) && parts.length == 4) {
            return fromParts(parts[1], parts[2], parts[3]);
        }
        throw new IllegalArgumentException("Invalid MessageId format: %s".formatted(offset));
    }

    @Override
    public int compareTo(Offset o) {
        Objects.requireNonNull(o, "Cannot compare with null Offset");

        if (o instanceof PulsarOffset other) {
            return messageId.compareTo(other.messageId);
        }

        throw new IllegalArgumentException(
            "Cannot compare different Offset types. Expected: %s, but got: %s".formatted(
                PulsarOffset.class.getName(),
                o.getClass().getName()
            )
        );
    }

    @Override
    public String toString() {
        return "mId:" + messageId.toString();
    }
}
