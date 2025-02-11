package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

@JsonTypeInfo (use = JsonTypeInfo.Id.NAME, property = "@queueType")
@JsonSubTypes ({@JsonSubTypes.Type (value = InternalQueueType.Main.class, name = "main"), @JsonSubTypes.Type (
    value = InternalQueueType.Retry.class, name = "retry"), @JsonSubTypes.Type (
        value = InternalQueueType.DeadLetter.class, name = "dead-letter"),})
public abstract sealed class InternalQueueType {

    @JsonIgnore
    public abstract InternalQueueCategory getCategory();

    @NoArgsConstructor (onConstructor_ = {@JsonCreator})
    public static final class Main extends InternalQueueType {

        public static final Main MAIN = new Main();

        @Override
        public InternalQueueCategory getCategory() {
            return InternalQueueCategory.MAIN;
        }

        @Override
        public boolean equals(final Object o) {
            if (o == this) {
                return true;
            }
            return o instanceof Main;
        }

        @Override
        public int hashCode() {
            return Main.class.hashCode();
        }

        @Override
        public String toString() {
            return "Main";
        }
    }


    @Getter
    @RequiredArgsConstructor (onConstructor_ = {@JsonCreator})
    public static final class Retry extends InternalQueueType {

        public static final Retry[] RETRY = new Retry[] {new Retry(1), new Retry(2), new Retry(3), new Retry(4),
            new Retry(5), new Retry(6), new Retry(7), new Retry(8), new Retry(9),};

        private final int retryCount;

        @Override
        public InternalQueueCategory getCategory() {
            return InternalQueueCategory.RETRY;
        }

        @Override
        public boolean equals(final Object o) {
            if (o == this) {
                return true;
            }
            if (!(o instanceof Retry other)) {
                return false;
            }
            return this.getRetryCount() == other.getRetryCount();
        }

        @Override
        public int hashCode() {
            final int PRIME = 59;
            int result = Retry.class.hashCode();
            result = result * PRIME + this.getRetryCount();
            return result;
        }

        @Override
        public String toString() {
            return "Retry-" + retryCount;
        }
    }


    @NoArgsConstructor (onConstructor_ = {@JsonCreator})
    public static final class DeadLetter extends InternalQueueType {

        public static final DeadLetter DEADLETTER = new DeadLetter();

        @Override
        public InternalQueueCategory getCategory() {
            return InternalQueueCategory.DEAD_LETTER;
        }

        @Override
        public boolean equals(final Object o) {
            if (o == this) {
                return true;
            }
            return o instanceof DeadLetter;
        }

        @Override
        public int hashCode() {
            return DeadLetter.class.hashCode();
        }

        @Override
        public String toString() {
            return "DeadLetter";
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
