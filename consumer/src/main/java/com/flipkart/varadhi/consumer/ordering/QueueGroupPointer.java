package com.flipkart.varadhi.consumer.ordering;

public class QueueGroupPointer {
    MessagePointer producePointer;
    MessagePointer consumePointer;

    public boolean hasLag() {
        return producePointer.compareTo(consumePointer) > 0;
    }
}
