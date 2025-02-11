package com.flipkart.varadhi.entities;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

@Getter
public class UnsidelineRequest {
    public static final long UNSPECIFIED_TS = 0L;
    private long latestFailedAt;
    private List<String> groupIds;
    private List<String> messageIds;

    private UnsidelineRequest(long failedAt, List<String> groupIds, List<String> messageIds) {
        this.latestFailedAt = failedAt;
        this.groupIds = groupIds;
        this.messageIds = messageIds;
    }

    public static UnsidelineRequest ofFailedAt(long latestFailedAt) {
        return new UnsidelineRequest(latestFailedAt, new ArrayList<>(), new ArrayList<>());
    }

    public static UnsidelineRequest ofGroupIds(List<String> groupIds) {
        return new UnsidelineRequest(UNSPECIFIED_TS, groupIds, new ArrayList<>());
    }

    public static UnsidelineRequest ofMessageIds(List<String> messageIds) {
        return new UnsidelineRequest(UNSPECIFIED_TS, new ArrayList<>(), messageIds);
    }

    public void validate(int maxGroupIds, int maxMessageIds) {
        if (latestFailedAt == UNSPECIFIED_TS && messageIds.isEmpty() && groupIds.isEmpty()) {
            throw new IllegalArgumentException("At least one unsideline criteria needs to be specified.");
        }
        if ((latestFailedAt != UNSPECIFIED_TS && (!groupIds.isEmpty() || !messageIds.isEmpty())) || (latestFailedAt
                                                                                                     == UNSPECIFIED_TS
                                                                                                     && (!groupIds.isEmpty()
                                                                                                         && !messageIds.isEmpty()))) {
            throw new IllegalArgumentException("Only one of the unsideline criteria should be specified.");
        }

        if (groupIds.size() > maxGroupIds) {
            throw new IllegalArgumentException(
                "Number of groupIds in one API call cannot be more than " + maxGroupIds + "."
            );
        }
        if (messageIds.size() > maxMessageIds) {
            throw new IllegalArgumentException(
                "Number of messageIds in one API call cannot be more than " + maxMessageIds + "."
            );
        }
    }
}
