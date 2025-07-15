package com.flipkart.varadhi.spi.inmemory;

import com.flipkart.varadhi.entities.Offset;

public record InMemoryOffset(int partition, long offset) implements Offset {

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

    @Override
    public String toString() {
        return partition + ":" + offset;
    }
}
