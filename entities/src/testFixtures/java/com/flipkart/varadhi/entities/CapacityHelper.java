package com.flipkart.varadhi.entities;

public class CapacityHelper {
    public static CapacityPolicy getDefault() {
        return new CapacityPolicy(100, 100);
    }
}
