package com.flipkart.varadhi.spi.services;

import com.flipkart.varadhi.entities.Offset;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Represent a batch of messages polled from topics using a consumer object.
 */
public interface PolledMessages<O extends Offset> extends Iterable<PolledMessage<O>> {
    int getCount();

    @RequiredArgsConstructor
    class ArrayBacked<O extends Offset> implements PolledMessages<O> {

        private final ArrayList<PolledMessage<O>> msgs;

        @Override
        public int getCount() {
            return msgs.size();
        }

        @Override
        public Iterator<PolledMessage<O>> iterator() {
            return msgs.iterator();
        }
    }
}
