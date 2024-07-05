package com.flipkart.varadhi.entities.cluster;

public interface Operation {
    String getId();

    boolean isDone();

    void markFail(String error);

    void markCompleted();
}
