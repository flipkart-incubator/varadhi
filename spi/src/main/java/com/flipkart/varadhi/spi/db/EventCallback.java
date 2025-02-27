package com.flipkart.varadhi.spi.db;

@FunctionalInterface
public interface EventCallback {
    void onEvent(IEventMarker marker);
}
