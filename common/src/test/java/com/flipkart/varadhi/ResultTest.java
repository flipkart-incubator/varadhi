package com.flipkart.varadhi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ResultTest {

    @Test
    public void testResult() {
        Result<String> r;
        r = Result.of("some data");
        Assertions.assertTrue(r.hasResult());
        Assertions.assertFalse(r.hasFailed());
        Assertions.assertEquals("some data", r.result());
        Assertions.assertNull(r.cause());

        IllegalArgumentException e = new IllegalArgumentException("some error");
        r = Result.of(e);
        Assertions.assertFalse(r.hasResult());
        Assertions.assertTrue(r.hasFailed());
        Assertions.assertNull(r.result());
        Assertions.assertEquals(e, r.cause());

        r = Result.of("some data", null);
        Assertions.assertTrue(r.hasResult());
        Assertions.assertFalse(r.hasFailed());
        Assertions.assertEquals("some data", r.result());
        Assertions.assertNull(r.cause());

        r = Result.of(null, e);
        Assertions.assertFalse(r.hasResult());
        Assertions.assertTrue(r.hasFailed());
        Assertions.assertNull(r.result());
        Assertions.assertEquals(e, r.cause());

        r = Result.of(null, null);
        Assertions.assertFalse(r.hasResult());
        Assertions.assertFalse(r.hasFailed());
        Assertions.assertNull(r.result());
        Assertions.assertNull(r.cause());
        IllegalArgumentException ee = Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> Result.of("some data", e)
        );
        Assertions.assertEquals("Both result and cause can't be non null.", ee.getMessage());
    }
}
