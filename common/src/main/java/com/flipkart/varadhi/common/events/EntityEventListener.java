package com.flipkart.varadhi.common.events;

public interface EntityEventListener {
    void onChange(EntityEvent<?> event);
}
