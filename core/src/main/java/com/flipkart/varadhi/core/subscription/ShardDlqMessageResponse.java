package com.flipkart.varadhi.core.subscription;

import com.flipkart.varadhi.entities.web.DlqMessage;
import lombok.Data;

import java.util.List;

@Data
public class ShardDlqMessageResponse {
    private final List<DlqMessage> messages;
    private final String nextPageMarker;
}
