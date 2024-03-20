package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Offset;

/**
 * Represent a batch of messages polled from topics using a consumer object.
 */
public interface PolledMessages<O extends Offset> extends Iterable<PolledMessage<O>> {
    int getCount();
}
