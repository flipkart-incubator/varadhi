package com.flipkart.varadhi.pulsar.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

public class PropertyHelperTest {

    @Test
    public void testPropertyEncodingDecoding() {
        List<String> values = List.of("one", "two", "three", "four");
        String encodedValues = PropertyHelper.encodePropertyValues(values);
        Collection<String> decodedValues = PropertyHelper.decodePropertyValues(encodedValues);
        Assertions.assertArrayEquals(values.toArray(), decodedValues.toArray());
    }
}
