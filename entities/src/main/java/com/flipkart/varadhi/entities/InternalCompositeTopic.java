package com.flipkart.varadhi.entities;


import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A wrapper on the storage topic. In future this class will handle the adding additional storage topics for the purpose
 * of increasing partition count without affecting ordering.
 * This concept is internal and is never exposed to the user.
 */
@Getter
@AllArgsConstructor
public class InternalCompositeTopic {
    private StorageTopic[] storageTopics;
    private int produceIndex;
    @Setter
    private TopicState topicState;

    public static InternalCompositeTopic of(StorageTopic storageTopic) {
        return new InternalCompositeTopic(new StorageTopic[] {storageTopic}, 0, TopicState.Producing);
    }

    @JsonIgnore
    public StorageTopic getTopicToProduce() {
        return storageTopics[produceIndex];
    }

    @JsonIgnore
    public List<StorageTopic> getActiveTopics() {
        return new ArrayList<>(Arrays.asList(storageTopics));
    }
}
