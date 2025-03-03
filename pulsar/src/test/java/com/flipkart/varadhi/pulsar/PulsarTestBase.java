package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.entities.MessageHeaderUtils;

import com.flipkart.varadhi.entities.utils.HeaderUtils;

public class PulsarTestBase {

    public static void setUp() {
        if (!HeaderUtils.getInstance().isInitialized()) {
            HeaderUtils.init(MessageHeaderUtils.fetchDummyHeaderConfiguration());
        }
    }
}
