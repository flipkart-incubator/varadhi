package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.spi.services.Consumer;
import com.flipkart.varadhi.spi.services.PolledMessage;
import com.flipkart.varadhi.spi.services.PolledMessages;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
class UnGroupedMessageSrcTest {

    @Test
    void testMessageSrcDriver() {
        Consumer<DummyOffset> consumer = new DummyConsumer();
        UnGroupedMessageSrc<DummyOffset> messageSrc = new UnGroupedMessageSrc<>(consumer);
        MessageTracker[] messages = new MessageTracker[3];
        Integer res = messageSrc.nextMessages(messages).join();
        for (MessageTracker message : messages) {
            message.onConsumed(MessageConsumptionStatus.SENT);
        }
        Integer res2 = messageSrc.nextMessages(messages).join();
        for (MessageTracker message : messages) {
            message.onConsumed(MessageConsumptionStatus.SENT);
        }
        Assertions.assertEquals(3, res);
        Assertions.assertEquals(1, res2);
    }

    static class DummyConsumer implements Consumer<DummyOffset> {
        @Override
        public CompletableFuture<PolledMessages<DummyOffset>> receiveAsync() {
            return CompletableFuture.supplyAsync(() -> new PolledMessages<>() {
                final List<String> messages = List.of("m1", "m2", "m3", "m4");

                @Override
                public int getCount() {
                    return messages.size();
                }

                @Override
                public Iterator<PolledMessage<DummyOffset>> iterator() {
                    Iterator<String> iter = messages.iterator();
                    return new Iterator<>() {
                        @Override
                        public boolean hasNext() {
                            return iter.hasNext();
                        }

                        @Override
                        public PolledMessage<DummyOffset> next() {
                            var message = iter.next();
                            return new PolledMessage<>() {
                                @Override
                                public String getTopicName() {
                                    return null;
                                }

                                @Override
                                public int getPartition() {
                                    return 0;
                                }

                                @Override
                                public DummyOffset getOffset() {
                                    return new DummyOffset(1);
                                }

                                @Override
                                public byte[] getPayload() {
                                    return message.getBytes(StandardCharsets.UTF_8);
                                }

                                @Override
                                public void release() {

                                }
                            };
                        }
                    };
                }
            });
        }

        @Override
        public CompletableFuture<Void> commitCumulativeAsync(PolledMessage<DummyOffset> message) {
            return null;
        }

        @Override
        public CompletableFuture<Void> commitIndividualAsync(PolledMessage<DummyOffset> message) {
            return CompletableFuture.supplyAsync(() -> {
                log.info("Committing message: {}", message.getPayload());
                return null;
            });
        }

        @Override
        public void close() throws Exception {

        }
    }

    public static class DummyOffset implements Offset {
        int offset;

        public DummyOffset(int offset) {
            this.offset = offset;
        }

        @Override
        public int compareTo(Offset o) {
            return offset - ((DummyOffset) o).offset;
        }
    }

}
