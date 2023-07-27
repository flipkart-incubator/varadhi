package com.flipkart.varadhi.produce;

import lombok.Getter;

@Getter
public enum MsgProduceStatus {

    Failed("Failed"),
    Throttled("Throttled"),
    Blocked("Blocked"),
    NotAllowed("NotAllowed"),
    Success("Succeeded");

    private String tagCategory;

    private MsgProduceStatus(String tagCategory) {
        this.tagCategory = tagCategory;
    }

}
