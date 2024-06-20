package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@queueType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = InternalQueueType.Main.class, name = "main"),
        @JsonSubTypes.Type(value = InternalQueueType.Retry.class, name = "retry"),
        @JsonSubTypes.Type(value = InternalQueueType.DeadLetter.class, name = "dead-letter"),
})
public abstract sealed class InternalQueueType {

    @JsonIgnore
    public abstract InternalQueueCategory getCategory();

    @NoArgsConstructor(onConstructor_ = {@JsonCreator})
    final public static class Main extends InternalQueueType {

        public static final Main MAIN = new Main();

        @Override
        public InternalQueueCategory getCategory() {
            return InternalQueueCategory.MAIN;
        }

        public boolean equals(final Object o) {
            if (o == this) {
                return true;
            }
            return o instanceof Main;
        }

        public int hashCode() {
            return Main.class.hashCode();
        }
    }

    @Getter
    @RequiredArgsConstructor(onConstructor_ = {@JsonCreator})
    final public static class Retry extends InternalQueueType {

        public static final Retry[] RETRY = new Retry[]{
                new Retry(1), new Retry(2), new Retry(3),
                new Retry(4), new Retry(5), new Retry(6),
                new Retry(7), new Retry(8), new Retry(9),
        };

        private final int retryCount;

        @Override
        public InternalQueueCategory getCategory() {
            return InternalQueueCategory.RETRY;
        }

        public boolean equals(final Object o) {
            if (o == this) {
                return true;
            }
            if (!(o instanceof Retry other)) {
                return false;
            }
            return this.getRetryCount() == other.getRetryCount();
        }

        public int hashCode() {
            final int PRIME = 59;
            int result = Retry.class.hashCode();
            result = result * PRIME + this.getRetryCount();
            return result;
        }
    }

    @NoArgsConstructor(onConstructor_ = {@JsonCreator})
    final public static class DeadLetter extends InternalQueueType {

        public static final DeadLetter DEADLETTER = new DeadLetter();

        @Override
        public InternalQueueCategory getCategory() {
            return InternalQueueCategory.DEAD_LETTER;
        }

        public boolean equals(final Object o) {
            if (o == this) {
                return true;
            }
            return o instanceof DeadLetter;
        }

        public int hashCode() {
            return DeadLetter.class.hashCode();
        }
    }

    public static Main mainType() {
        return Main.MAIN;
    }

    public static DeadLetter deadLetterType() {
        return DeadLetter.DEADLETTER;
    }

    public static Retry retryType(int retryCount) {
        if (retryCount < 1 || retryCount > 9) {
            throw new IllegalArgumentException("Invalid retry count: " + retryCount);
        }
        return Retry.RETRY[retryCount - 1];
    }
}
