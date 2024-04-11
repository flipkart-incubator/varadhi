package com.flipkart.varadhi.consumer;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

public abstract sealed class InternalQueueType {

    abstract InternalQueueCategory getCategory();


    final public static class Main extends InternalQueueType {

        @Override
        InternalQueueCategory getCategory() {
            return InternalQueueCategory.MAIN;
        }
    }

    @Getter
    @RequiredArgsConstructor
    final public static class Retry extends InternalQueueType {

        private final int retryCount;

        @Override
        InternalQueueCategory getCategory() {
            return InternalQueueCategory.RETRY;
        }
    }

    final public static class DeadLetter extends InternalQueueType {

        @Override
        InternalQueueCategory getCategory() {
            return InternalQueueCategory.DEAD_LETTER;
        }
    }
}
