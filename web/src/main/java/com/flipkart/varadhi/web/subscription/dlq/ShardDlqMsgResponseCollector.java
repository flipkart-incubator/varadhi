package com.flipkart.varadhi.web.subscription.dlq;


import com.flipkart.varadhi.core.subscription.ShardDlqMessageResponse;
import com.flipkart.varadhi.entities.web.DlqMessagesResponse;
import com.flipkart.varadhi.entities.web.DlqPageMarker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ShardDlqMsgResponseCollector {
    private final Map<Integer, String> errors;
    private final DlqPageMarker pageMarker;

    public ShardDlqMsgResponseCollector() {
        this.errors = new HashMap<>();
        this.pageMarker = new DlqPageMarker(new HashMap<>());
    }

    public void collectShardResponse(int shardId, Throwable t, ShardDlqMessageResponse r) {
        if (t != null) {
            errors.put(shardId, t.getMessage());
        } else if (r != null && r.getNextPageMarker() != null) {
            pageMarker.addShardMarker(shardId, r.getNextPageMarker());
        }
    }

    public DlqMessagesResponse toAggregatedResponse(Throwable t) {
        DlqMessagesResponse response = DlqMessagesResponse.of(new ArrayList<>());
        if (t != null) {
            response.setError(t.getMessage());
        }
        if (!errors.isEmpty()) {
            response.setError(
                String.join(
                    ",",
                    errors.entrySet().stream().map(e -> String.format("%d:%s", e.getKey(), e.getValue())).toList()
                )
            );
        } else if (pageMarker.hasMarkers()) {
            response.setNextPage(pageMarker.toString());
        }
        return response;
    }
}
