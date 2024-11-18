package com.flipkart.varadhi.entities;

import lombok.Data;

import java.util.List;

@Data
public class GetMessagesResponse {
    private int shardId;
    private List<Message> messages;
    private String error;

    public static GetMessagesResponse of(int shardId) {
        GetMessagesResponse response = new GetMessagesResponse();
        response.setShardId(shardId);
        return response;
    }
}
