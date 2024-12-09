package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.spi.services.DummyProducer.DummyOffset;
import com.google.common.collect.Multimap;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class DummyConsumer implements Consumer<DummyOffset> {

    // allows to confirm the acknowledgement of messages
    private final Map<String, Boolean> messages;
    private boolean isCalled = false;

    public DummyConsumer(List<String> messages) {
        this.messages = messages.stream().map(message -> Map.entry(message, false))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public CompletableFuture<PolledMessages<DummyOffset>> receiveAsync() {
        if (isCalled) {
            // return empty iterator if the consumer is already called
            return CompletableFuture.completedFuture(new PolledMessages<>() {
                @Override
                public int getCount() {
                    return 0;
                }

                @Override
                public Iterator<PolledMessage<DummyOffset>> iterator() {
                    return new Iterator<>() {
                        @Override
                        public boolean hasNext() {
                            return false;
                        }

                        @Override
                        public PolledMessage<DummyOffset> next() {
                            return null;
                        }
                    };
                }
            });
        }

        // mark the consumer as called to avoid multiple calls
        isCalled = true;
        return CompletableFuture.supplyAsync(() -> new PolledMessages<>() {
            @Override
            public int getCount() {
                return messages.size();
            }

            @Override
            public Iterator<PolledMessage<DummyOffset>> iterator() {
                Iterator<String> iter = messages.keySet().iterator();
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public PolledMessage<DummyOffset> next() {
                        String message = iter.next();
                        return new PolledMessage<>() {
                            @Override
                            public long getProducedTimestampMs() {
                                return 0;
                            }

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
                            public String getMessageId() {
                                return null;
                            }

                            @Override
                            public String getGroupId() {
                                return null;
                            }

                            @Override
                            public boolean hasHeader(String key) {
                                return false;
                            }

                            @Override
                            public String getHeader(String key) {
                                return null;
                            }

                            @Override
                            public List<String> getHeaders(String key) {
                                return null;
                            }

                            @Override
                            public byte[] getPayload() {
                                return message.getBytes(StandardCharsets.UTF_8);
                            }

                            @Override
                            public Multimap<String, String> getHeaders() {
                                return null;
                            }

                            @Override
                            public void release() {
                                // no op
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
        messages.put(new String(message.getPayload(), StandardCharsets.UTF_8), true);
        return null;
    }

    public int getCommittedMessagesCount() {
        return messages.entrySet().stream().filter(Map.Entry::getValue).mapToInt(entry -> 1).sum();
    }

    public List<String> getCommittedMessages() {
        return messages.entrySet().stream().filter(Map.Entry::getValue).map(Map.Entry::getKey).toList();
    }

    public void permitMoreMessages() {
        isCalled = false;
    }

    @Override
    public void close() {
        // no op
    }

    // Simulate a slow consumer that takes a delay to return each batch of messages
    public static class SlowConsumer extends DummyConsumer {

        private final long delayInSeconds;

        public SlowConsumer(List<String> messages, int delayInSeconds) {
            super(messages);
            this.delayInSeconds = delayInSeconds;
        }

        @Override
        public CompletableFuture<PolledMessages<DummyOffset>> receiveAsync() {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(delayInSeconds * 1000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return super.receiveAsync().join();
            });
        }
    }
}
