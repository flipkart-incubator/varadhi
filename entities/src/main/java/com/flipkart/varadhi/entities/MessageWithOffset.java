package com.flipkart.varadhi.entities;

public interface MessageWithOffset<O extends Offset> extends Message {

    /**
     * @return the offset of this message in the partition.
     */
    O getOffset();
}
