package com.flipkart.varadhi.consumer.push;

import com.flipkart.varadhi.consumer.MessageTracker;

public record PushResponse(MessageTracker msg, boolean success) {
}
