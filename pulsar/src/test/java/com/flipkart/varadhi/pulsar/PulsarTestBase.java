package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.entities.StdHeaders;
import com.flipkart.varadhi.entities.TestStdHeaders;
import org.junit.jupiter.api.BeforeAll;

public class PulsarTestBase {

    @BeforeAll
    public static void setUp() {
        if (!StdHeaders.isGlobalInstanceInitialized()) {
            StdHeaders.init(TestStdHeaders.get());
        }
    }
}
