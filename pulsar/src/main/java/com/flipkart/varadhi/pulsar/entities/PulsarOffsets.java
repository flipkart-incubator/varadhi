package com.flipkart.varadhi.pulsar.entities;


import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.flipkart.varadhi.pulsar.util.PulsarOffsetsDeserializer;
import com.flipkart.varadhi.pulsar.util.PulsarOffsetsSerializer;
import lombok.Data;

import java.util.List;

@JsonSerialize(using = PulsarOffsetsSerializer.class)
@JsonDeserialize(using = PulsarOffsetsDeserializer.class)
@Data
public class PulsarOffsets {
    private final List<PulsarOffset> offsets;
}
