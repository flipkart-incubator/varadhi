package com.flipkart.varadhi.qos;

import com.flipkart.varadhi.entities.ratelimit.SuppressionData;

public interface RLObserver {
    void update(SuppressionData suppressionData);
}
