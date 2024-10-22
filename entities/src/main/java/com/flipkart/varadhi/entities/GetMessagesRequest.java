package com.flipkart.varadhi.entities;


import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class GetMessagesRequest {
    public static final long UNSPECIFIED_TS = 0L;
    public static final long LATEST_TS = Long.MAX_VALUE;

    //TODO:: evaluate if earliestFailedAt should be per shard level to support pagination kind of semantics.
    private long earliestFailedAt = 0L;
    private List<Offset> offsets = new ArrayList<>();
    private int limit = -1;

    public void validate(int maxLimit) {
        if (earliestFailedAt == UNSPECIFIED_TS && offsets.isEmpty()) {
            throw new IllegalArgumentException("At least one get messages criteria needs to be specified.");
        }
        if (earliestFailedAt != UNSPECIFIED_TS && !offsets.isEmpty() ) {
            throw new IllegalArgumentException(
                    "Only one of the get messages criteria should be specified.");
        }
        if (offsets.size() > maxLimit) {
            throw new IllegalArgumentException("Number of offsets cannot be more than " + maxLimit + ".");
        }

        if (limit > maxLimit) {
            throw new IllegalArgumentException("Limit cannot be more than " + maxLimit + ".");
        }
    }
}
