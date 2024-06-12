package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.*;
import lombok.experimental.FieldDefaults;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "@queueType")
@JsonSubTypes({
        @JsonSubTypes.Type(value = InternalQueueType.Main.class, name = "main"),
        @JsonSubTypes.Type(value = InternalQueueType.Retry.class, name = "retry"),
        @JsonSubTypes.Type(value = InternalQueueType.DeadLetter.class, name = "dead-letter"),
})
public abstract sealed class InternalQueueType {

    @JsonIgnore
    public abstract InternalQueueCategory getCategory();

    @NoArgsConstructor(onConstructor = @__(@JsonCreator))
    final public static class Main extends InternalQueueType {

        @Override
        public InternalQueueCategory getCategory() {
            return InternalQueueCategory.MAIN;
        }
    }

    @Getter
    @RequiredArgsConstructor(onConstructor = @__(@JsonCreator))
    final public static class Retry extends InternalQueueType {

        private final int retryCount;

        @Override
        public InternalQueueCategory getCategory() {
            return InternalQueueCategory.RETRY;
        }
    }

    @NoArgsConstructor(onConstructor = @__(@JsonCreator))
    final public static class DeadLetter extends InternalQueueType {

        @Override
        public InternalQueueCategory getCategory() {
            return InternalQueueCategory.DEAD_LETTER;
        }
    }
}
