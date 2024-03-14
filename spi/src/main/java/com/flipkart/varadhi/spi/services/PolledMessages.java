package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Offset;
import lombok.AllArgsConstructor;

import java.util.List;

/**
 * Represent a batch of messages polled from topics using a consumer object.
 */
@AllArgsConstructor
public class PolledMessages<O extends Offset> {

    private final List<PartitionMessages<O>> messages;
}
