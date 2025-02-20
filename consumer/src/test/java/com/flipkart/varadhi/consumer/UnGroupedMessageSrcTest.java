package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.InternalQueueType;
import com.flipkart.varadhi.spi.services.DummyConsumer;
import com.flipkart.varadhi.spi.services.DummyProducer.DummyOffset;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class UnGroupedMessageSrcTest {

    private ConsumerMetrics dummyMetrics = new ConsumerMetrics(
        new SimpleMeterRegistry(),
        "test",
        0,
        new InternalQueueType[] {InternalQueueType.mainType()}
    );

    @Test
    void testShouldNotBufferMessagesWhenConsumerReturnExactBatch() {
        List<String> messages = List.of("a", "b", "c");
        MessageTracker[] messageTrackers = new MessageTracker[messages.size()];

        DummyConsumer consumer = new DummyConsumer(messages);
        UnGroupedMessageSrc<DummyOffset> messageSrc = new UnGroupedMessageSrc<>(
            InternalQueueType.mainType(),
            consumer,
            dummyMetrics
        );
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
        UnGroupedMessageSrc<DummyOffset> messageSrc = new UnGroupedMessageSrc<>(
            InternalQueueType.mainType(),
            consumer,
            dummyMetrics
        );
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
    void testUseOnlyBufferWhenPresent() {
        List<String> messages = List.of("a", "b", "c", "d", "e", "f");
        MessageTracker[] messageTrackers = new MessageTracker[4];

        DummyConsumer consumer = new DummyConsumer(messages);
        UnGroupedMessageSrc<DummyOffset> messageSrc = new UnGroupedMessageSrc<>(
            InternalQueueType.mainType(),
            consumer,
            dummyMetrics
        );
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
        messageTrackers = new MessageTracker[2];
        consumer.permitMoreMessages();
        res = messageSrc.nextMessages(messageTrackers).join();

        assertEquals(messageTrackers.length, res);

        List<String> nextMessages = new ArrayList<>();
        for (MessageTracker message : messageTrackers) {
            nextMessages.add(new String(message.getMessage().getPayload()));
        }

        assertListEquals(List.of("e", "f"), nextMessages);
    }

    @Test
    void testUseOnlyBufferNoCallToConsumer() {
        List<String> messages = List.of("a", "b", "c", "d", "e", "f");
        MessageTracker[] messageTrackers = new MessageTracker[3];

        DummyConsumer consumer = new DummyConsumer(messages);
        UnGroupedMessageSrc<DummyOffset> messageSrc = new UnGroupedMessageSrc<>(
            InternalQueueType.mainType(),
            consumer,
            dummyMetrics
        );
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

    @Test
    void testConcurrencyInConsumerFetchNotAllowed() {
        // we will simulate a slow consumer, now when continuous calls are made to fetch messages, 2nd one should immediately return with 0 messages
        List<String> messages = List.of("a", "b", "c", "d", "e", "f");
        MessageTracker[] messageTrackers = new MessageTracker[3];

        DummyConsumer.SlowConsumer consumer = new DummyConsumer.SlowConsumer(messages, 3);
        UnGroupedMessageSrc<DummyOffset> messageSrc = new UnGroupedMessageSrc<>(
            InternalQueueType.mainType(),
            consumer,
            dummyMetrics
        );
        var f1 = messageSrc.nextMessages(messageTrackers);

        try {
            assertFalse(f1.isDone());
            var f2 = messageSrc.nextMessages(messageTrackers);
            Assertions.fail("concurrent invocation is not expected.");
        } catch (IllegalStateException e) {
            // expected.
        }

        assertEquals(messageTrackers.length, f1.join());

        // since f1 is completed now, next invocation should return remaining messages
        messageTrackers = new MessageTracker[3];
        assertEquals(3, messageSrc.nextMessages(messageTrackers).join());
    }

    private void assertListEquals(List<String> expect, List<String> actual) {
        assertTrue(expect.size() == actual.size() && expect.containsAll(actual) && actual.containsAll(expect));
    }
}
