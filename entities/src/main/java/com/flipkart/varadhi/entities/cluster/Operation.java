package com.flipkart.varadhi.entities.cluster;

public interface Operation {
    String getId();

    State getState();

    String getErrorMsg();

    boolean isDone();

    boolean hasFailed();

    void markFail(String error);

    void markCompleted();

    enum State {
        ERRORED, COMPLETED, IN_PROGRESS
    }
}
