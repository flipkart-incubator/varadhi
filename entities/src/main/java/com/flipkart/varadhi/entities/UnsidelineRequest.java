package com.flipkart.varadhi.entities;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class UnsidelineRequest {
    public static final long UNSPECIFIED_TS = 0L;

    private long latestFailedAt = 0;
    private List<String> groupIds = new ArrayList<>();
    private List<String> messageIds = new ArrayList<>();

    public void validate(int maxGroupIds, int maxMessageIds) {
        if (latestFailedAt == UNSPECIFIED_TS && messageIds.isEmpty() && groupIds.isEmpty()) {
            throw new IllegalArgumentException("At least one unsideline criteria needs to be specified.");
        }
        if ((latestFailedAt != UNSPECIFIED_TS && (!groupIds.isEmpty() || !messageIds.isEmpty()))
                || (latestFailedAt == UNSPECIFIED_TS && (!groupIds.isEmpty() && !messageIds.isEmpty()))) {
            throw new IllegalArgumentException(
                    "Only one of the unsideline criteria should be specified.");
        }

        if (groupIds.size() > maxGroupIds) {
            throw new IllegalArgumentException(
                    "Number of groupIds in one API call cannot be more than " + maxGroupIds + ".");
        }
        if (messageIds.size() > maxMessageIds) {
            throw new IllegalArgumentException(
                    "Number of messageIds in one API call cannot be more than " + maxMessageIds + ".");
        }
    }
}
