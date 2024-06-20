package com.flipkart.varadhi.entities.cluster;


//TODO:: Rename this appropriately.
public interface GroupOperation {
    public String getGroupId();
    public String getId();
    public boolean isDone();
    public void markFail(String error);

}
