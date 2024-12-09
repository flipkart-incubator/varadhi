package com.flipkart.varadhi.pulsar.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.pulsar.util.PulsarOffsetDeserializer;
import com.flipkart.varadhi.pulsar.util.PulsarOffsetSerializer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.bouncycastle.util.Strings;


@JsonSerialize(using = PulsarOffsetSerializer.class)
@JsonDeserialize(using = PulsarOffsetDeserializer.class)
public class PulsarOffset implements Offset {
    private final MessageId messageId;

    public PulsarOffset(MessageId messageId) {
        this.messageId = messageId;
    }

    public MessageId getMessageId() {
        return this.messageId;
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
        return "mId:" + messageId.toString();
    }

    public static PulsarOffset fromString(String offset) {
        String[] parts = Strings.split(offset, ':');
        if ("mId".equals(parts[0]) && parts.length == 4) {
            return new PulsarOffset(fromParts(parts[1], parts[2], parts[3]));
        }
        throw new IllegalArgumentException("Unknown PulsarOffset format: %s".formatted(offset));
    }

    private static MessageId fromParts(String ledgerId, String entryId, String partitionId) {
        long ledger = Long.parseLong(ledgerId);
        long entry = Long.parseLong(entryId);
        int partitionIdx = Integer.parseInt(partitionId);
        return new MessageIdImpl(ledger, entry, partitionIdx);
    }


}
