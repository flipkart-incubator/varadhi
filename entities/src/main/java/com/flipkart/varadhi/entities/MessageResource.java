package com.flipkart.varadhi.entities;


import lombok.Getter;

@Getter
public class MessageResource {
    private String messageId;
    private String groupId;
    private byte[] payload;


    public Message getMessageToProduce() {
        return new Message(messageId, groupId, payload, null);
    }
}
