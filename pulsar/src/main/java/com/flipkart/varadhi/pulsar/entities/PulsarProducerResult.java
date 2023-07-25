package com.flipkart.varadhi.pulsar.entities;

import com.flipkart.varadhi.entities.ProduceResult;
import com.flipkart.varadhi.entities.ProducerResult;

public class PulsarProducerResult extends ProducerResult {

    @Override
    public int compareTo(ProduceResult o) {
        return 0;
    }
}
