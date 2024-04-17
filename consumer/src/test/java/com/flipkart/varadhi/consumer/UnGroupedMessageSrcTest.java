package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.spi.services.DummyConsumer;
import com.flipkart.varadhi.spi.services.DummyProducer.DummyOffset;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class UnGroupedMessageSrcTest {

    @Test
    void testShouldNotBufferMessagesWhenConsumerReturnExactBatch() {
        List<String> messages = List.of("a", "b", "c");
        MessageTracker[] messageTrackers = new MessageTracker[messages.size()];

        DummyConsumer consumer = new DummyConsumer(messages);
        UnGroupedMessageSrc<DummyOffset> messageSrc = new UnGroupedMessageSrc<>(consumer);
        Integer res = messageSrc.nextMessages(messageTrackers).join();

        assertEquals(3, res);

        for (MessageTracker message : messageTrackers) {
            message.onConsumed(MessageConsumptionStatus.SENT);
        }

        assertEquals(3, consumer.getCommittedMessagesCount());

        messageTrackers = new MessageTracker[messages.size()];
        res = messageSrc.nextMessages(messageTrackers).join();
        assertEquals(0, res);
    }

    @Test
    void testShouldBufferMessagesWhenConsumerReturnMoreThanTrackerSize() {
        List<String> messages = List.of("a", "b", "c", "d", "e");
        MessageTracker[] messageTrackers = new MessageTracker[3];

        DummyConsumer consumer = new DummyConsumer(messages);
        UnGroupedMessageSrc<DummyOffset> messageSrc = new UnGroupedMessageSrc<>(consumer);
        Integer res = messageSrc.nextMessages(messageTrackers).join();

        assertEquals(3, res);

        List<String> committedList = new ArrayList<>();
        for (MessageTracker message : messageTrackers) {
            message.onConsumed(MessageConsumptionStatus.SENT);
            committedList.add(new String(message.getMessage().getPayload()));
        }

        assertEquals(3, consumer.getCommittedMessagesCount());
        assertListEquals(committedList, consumer.getCommittedMessages());


        // now it should fetch buffered messages
        messageTrackers = new MessageTracker[3];
        res = messageSrc.nextMessages(messageTrackers).join();

        assertEquals(2, res);

        for (int i = 0; i < res; i++) {
            messageTrackers[i].onConsumed(MessageConsumptionStatus.SENT);
            committedList.add(new String(messageTrackers[i].getMessage().getPayload()));
        }

        assertEquals(5, consumer.getCommittedMessagesCount());
        assertListEquals(committedList, consumer.getCommittedMessages());
    }

    @Test
    void testUseBufferAndConsumer() {
        List<String> messages = List.of("a", "b", "c", "d", "e", "f");
        MessageTracker[] messageTrackers = new MessageTracker[4];

        DummyConsumer consumer = new DummyConsumer(messages);
        UnGroupedMessageSrc<DummyOffset> messageSrc = new UnGroupedMessageSrc<>(consumer);
        Integer res = messageSrc.nextMessages(messageTrackers).join();

        assertEquals(4, res);

        List<String> committedList = new ArrayList<>();
        // a, b, c, d
        for (MessageTracker message : messageTrackers) {
            message.onConsumed(MessageConsumptionStatus.SENT);
            committedList.add(new String(message.getMessage().getPayload()));
        }

        assertEquals(4, consumer.getCommittedMessagesCount());
        assertListEquals(committedList, consumer.getCommittedMessages());

        // e (from buffer), f (from buffer), a, b
        messageTrackers = new MessageTracker[4];
        consumer.permitMoreMessages();
        res = messageSrc.nextMessages(messageTrackers).join();

        assertEquals(4, res);

        List<String> nextMessages = new ArrayList<>();
        for (MessageTracker message : messageTrackers) {
            nextMessages.add(new String(message.getMessage().getPayload()));
        }

        assertListEquals(List.of("e", "f", "a", "b"), nextMessages);
    }

    @Test
    void testUseOnlyBufferNoCallToConsumer() {
        List<String> messages = List.of("a", "b", "c", "d", "e", "f");
        MessageTracker[] messageTrackers = new MessageTracker[3];

        DummyConsumer consumer = new DummyConsumer(messages);
        UnGroupedMessageSrc<DummyOffset> messageSrc = new UnGroupedMessageSrc<>(consumer);
        Integer res = messageSrc.nextMessages(messageTrackers).join();

        assertEquals(3, res);

        List<String> committedList = new ArrayList<>();
        // a, b, c
        for (MessageTracker message : messageTrackers) {
            message.onConsumed(MessageConsumptionStatus.SENT);
            committedList.add(new String(message.getMessage().getPayload()));
        }

        assertEquals(3, consumer.getCommittedMessagesCount());
        assertListEquals(committedList, consumer.getCommittedMessages());

        // d (from buffer), e (from buffer), f (from buffer)
        messageTrackers = new MessageTracker[3];
        res = messageSrc.nextMessages(messageTrackers).join();

        assertEquals(3, res);

        List<String> nextMessages = new ArrayList<>();
        for (MessageTracker message : messageTrackers) {
            nextMessages.add(new String(message.getMessage().getPayload()));
        }

        assertListEquals(List.of("d", "e", "f"), nextMessages);
    }

    private void assertListEquals(List<String> expect, List<String> actual) {
        assertTrue(expect.size() == actual.size() && expect.containsAll(actual) && actual.containsAll(expect));
    }
}
