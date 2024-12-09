package com.flipkart.varadhi.core.cluster.entities;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.flipkart.varadhi.entities.Offset;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor(onConstructor = @__(@JsonCreator))
public class ShardDlqMessageRequest {
    private long earliestFailedAt = 0L;
    private final String pageMarker;
    private int limit = -1;

    public ShardDlqMessageRequest(long earliestFailedAt, int limit) {
        this.earliestFailedAt = earliestFailedAt;
        this.pageMarker = null;
        this.limit = limit;
    }

    public ShardDlqMessageRequest(String pageMarker, int limit) {
        earliestFailedAt = 0L;
        this.pageMarker = pageMarker;
        this.limit = limit;
    }
}
