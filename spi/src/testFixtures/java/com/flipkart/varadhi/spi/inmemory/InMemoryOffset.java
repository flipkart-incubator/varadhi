package com.flipkart.varadhi.spi.inmemory;

import com.flipkart.varadhi.entities.Offset;

public class InMemoryOffset implements Offset {
    private final int partition;
    private final long offset;

    public InMemoryOffset(int partition, long offset) {
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public int compareTo(Offset o) {
        if (o instanceof InMemoryOffset) {
            InMemoryOffset other = (InMemoryOffset)o;
            if (this.partition != other.partition) {
                throw new IllegalArgumentException("Cannot compare offsets from different partitions");
            }
            return Long.compare(this.offset, other.offset);
        }
        throw new IllegalArgumentException("Cannot compare with non-InMemoryOffset type");
    }
}
