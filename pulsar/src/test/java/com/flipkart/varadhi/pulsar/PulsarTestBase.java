package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.entities.StdHeaders;
import com.flipkart.varadhi.entities.TestStdHeaders;

public class PulsarTestBase {

    public static void setUp() {
        if (!StdHeaders.isGlobalInstanceInitialized()) {
            StdHeaders.init(TestStdHeaders.get());
        }
    }
}
