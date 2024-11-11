package com.flipkart.varadhi.qos.entity;

/**
 * Single topic's traffic data
 */
public record TrafficData (
    String topic,
    long bytesIn,
    long rateIn
){
}
