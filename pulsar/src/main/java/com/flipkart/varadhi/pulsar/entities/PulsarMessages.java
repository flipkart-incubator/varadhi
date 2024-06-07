package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.spi.services.PolledMessage;
import com.flipkart.varadhi.spi.services.PolledMessages;
import jakarta.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;

import java.util.Iterator;

@RequiredArgsConstructor
public class PulsarMessages implements PolledMessages<PulsarOffset> {

    private final Messages<byte[]> msgs;

    @Override
    public int getCount() {
        return msgs.size();
    }

    @Override
    @Nonnull
    public Iterator<PolledMessage<PulsarOffset>> iterator() {
        Iterator<Message<byte[]>> it = msgs.iterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public PolledMessage<PulsarOffset> next() {
                return new PulsarMessage(it.next());
            }
        };
    }
}
