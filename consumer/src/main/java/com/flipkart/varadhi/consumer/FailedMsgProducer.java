package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.spi.services.Producer;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.CompletableFuture;

/**
 * Convenient wrapper of a producer to produce a "failed" message to RQs & DLQs. It also allows to produce
 * follow through messages that are simply following failures in previous message of the same group.
 */
@RequiredArgsConstructor
public class FailedMsgProducer implements Producer {

    public static final String FOLLOW_THROUGH_MSG_HEADER = "varadhi.msg.followed";

    private final Producer delegate;

    public static boolean isFollowThroughMsg(Message message) {
        return message.hasHeader(FOLLOW_THROUGH_MSG_HEADER);
    }

    @Override
    public CompletableFuture<Offset> produceAsync(Message message) {
        return delegate.produceAsync(message.withoutHeader(FOLLOW_THROUGH_MSG_HEADER));
    }

    public CompletableFuture<Offset> produceFollowThroughMsgAsync(Message msg, String previousOffset) {
        return delegate.produceAsync(msg.withHeader(FOLLOW_THROUGH_MSG_HEADER, previousOffset));
    }
}
