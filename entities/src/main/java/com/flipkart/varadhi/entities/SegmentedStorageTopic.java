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
public class SegmentedStorageTopic {

    private final StorageTopic[] storageTopics;

    private final int activeStorageTopicId;

    @JsonIgnore
    private final int produceIndex;

    //TODO: state should be moved to a separate class. This class is not meant to handle runtime state.
    @Setter
    private TopicState topicState;

    public static SegmentedStorageTopic of(StorageTopic storageTopic) {
        return new SegmentedStorageTopic(
            new StorageTopic[] {storageTopic},
            storageTopic.getId(),
            0,
            TopicState.Producing
        );
    }

    @JsonIgnore
    public StorageTopic getTopicToProduce() {
        return storageTopics[produceIndex];
    }

    @JsonIgnore
    public StorageTopic getTopic(int id) {
        return Arrays.stream(storageTopics)
                     .filter(topic -> topic.getId() == id)
                     .findFirst()
                     .orElseThrow(() -> new IllegalArgumentException("No topic found with id: " + id));
    }

    @JsonIgnore
    public List<StorageTopic> getActiveTopics() {
        return new ArrayList<>(Arrays.asList(storageTopics));
    }
}
