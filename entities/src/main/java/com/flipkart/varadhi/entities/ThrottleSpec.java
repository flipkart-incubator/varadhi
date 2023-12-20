package com.flipkart.varadhi.entities;

import lombok.Data;

@Data
public class ThrottleSpec {
    private final double factor;
    private final int waitSeconds;
    private final int pingSeconds;
    private final int stopAfterSeconds;
}
