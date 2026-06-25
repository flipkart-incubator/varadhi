package com.flipkart.varadhi.entities;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A wrapper on the storage topic. In future this class will handle the adding additional storage topics for the purpose
 * of increasing partition count without affecting ordering.
 * This concept is internal and is never exposed to the user.
 *
 * <p>This class describes the storage-layer layout only; the runtime per-region produce
 * {@link TopicState} now lives on the owning {@link VaradhiTopic} (see
 * {@link VaradhiTopic#getTopicState(RegionName)}), keeping this class free of mutable
 * runtime state.
 */
// Tolerate the legacy {@code topicState} property in already-persisted topic JSON: that
// runtime state now lives on the owning VaradhiTopic, so ignore it on read instead of failing.
@JsonIgnoreProperties (ignoreUnknown = true)
@Getter
@AllArgsConstructor
public class SegmentedStorageTopic {

    private final StorageTopic[] storageTopics;

    private final int activeStorageTopicId;

    @JsonIgnore
    private final int produceIndex;

    public static SegmentedStorageTopic of(StorageTopic storageTopic) {
        return new SegmentedStorageTopic(new StorageTopic[] {storageTopic}, storageTopic.getId(), 0);
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
