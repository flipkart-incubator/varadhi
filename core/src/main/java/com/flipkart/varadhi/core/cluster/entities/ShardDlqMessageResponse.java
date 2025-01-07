package com.flipkart.varadhi.core.cluster.entities;

import com.flipkart.varadhi.entities.DlqMessage;
import lombok.Data;

import java.util.List;

@Data
public class ShardDlqMessageResponse {
    private final List<DlqMessage> messages;
    private final String nextPageMarker;
}
