package com.flipkart.varadhi.spi.inmemory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.flipkart.varadhi.entities.StorageTopic;
import lombok.Getter;

@Getter
@JsonTypeName ("in-memory")
public class InMemoryStorageTopic extends StorageTopic {

    private final int partitions;

    @JsonCreator
    public InMemoryStorageTopic(
        @JsonProperty ("id") int id,
        @JsonProperty ("name") String name,
        @JsonProperty ("partitions") int partitions
    ) {
        super(id, name);
        this.partitions = partitions;
    }
}
