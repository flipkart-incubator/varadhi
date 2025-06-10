package com.flipkart.varadhi.entities.web;

import lombok.Data;

import java.util.List;

@Data
public class DlqMessagesResponse {
    private List<DlqMessage> messages;
    private String error;
    private String nextPage;

    public static DlqMessagesResponse of(List<DlqMessage> messages) {
        DlqMessagesResponse response = new DlqMessagesResponse();
        response.setMessages(messages);
        return response;
    }

    public static DlqMessagesResponse of(DlqPageMarker pageMarkers, List<String> errors) {
        DlqMessagesResponse response = new DlqMessagesResponse();
        if (errors.isEmpty()) {
            response.nextPage = pageMarkers.toString();
        } else {
            response.error = String.join(",", errors);
        }
        return response;
    }
}
