package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Offset;
import lombok.AllArgsConstructor;

/**
 * Represent a batch of messages polled from a particular partition.
 */
@AllArgsConstructor
public class PartitionMessages<O extends Offset> {

    private final String topic;

    private final int partition;
    
    private final PolledMessage<O> messages;
}
