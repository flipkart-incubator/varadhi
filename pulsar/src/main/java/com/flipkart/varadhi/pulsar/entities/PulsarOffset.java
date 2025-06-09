package com.flipkart.varadhi.pulsar.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.pulsar.util.MessageIdDeserializer;
import com.flipkart.varadhi.pulsar.util.MessageIdSerializer;
import lombok.Getter;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;


@Getter
public class PulsarOffset implements Offset {

    @JsonSerialize (using = MessageIdSerializer.class)
    @JsonDeserialize (using = MessageIdDeserializer.class)
    private final MessageId messageId;

    public PulsarOffset(MessageId messageId) {
        this.messageId = messageId;
    }

    public static PulsarOffset of(String offset) {
        MessageId mId = messageIdFrom(offset);
        return new PulsarOffset(mId);
    }

    public static MessageId fromParts(String ledgerId, String entryId, String partitionId) {
        long ledger = Long.parseLong(ledgerId);
        long entry = Long.parseLong(entryId);
        int partitionIdx = Integer.parseInt(partitionId);
        return new MessageIdImpl(ledger, entry, partitionIdx);
    }

    public static MessageId messageIdFrom(String offset) {
        String[] parts = offset.split(":");
        if ("mId".equals(parts[0]) && parts.length == 4) {
            return fromParts(parts[1], parts[2], parts[3]);
        }
        throw new IllegalArgumentException("Unknown MessageId format: %s".formatted(offset));
    }

    @Override
    public int compareTo(Offset o) {
        if (null == o) {
            throw new IllegalArgumentException("Can not compare null Offset.");
        }
        if (o instanceof PulsarOffset) {
            return messageId.compareTo(((PulsarOffset)o).messageId);
        } else {
            throw new IllegalArgumentException(
                String.format(
                    "Can not compare different Offset types. Expected Offset is %s, given  %s.",
                    PulsarOffset.class.getName(),
                    o.getClass().getName()
                )
            );
        }
    }

    @Override
    public String toString() {
        return "mId:" + messageId.toString();
    }
}
