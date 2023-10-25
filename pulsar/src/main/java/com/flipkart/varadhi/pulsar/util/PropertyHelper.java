package com.flipkart.varadhi.pulsar.util;

import java.util.Arrays;
import java.util.Collection;

import static com.flipkart.varadhi.pulsar.Constants.PROPERTY_MULTI_VALUE_SEPARATOR;

public class PropertyHelper {

    //TODO:: This needs value sanitization or another form of encoding.
    public static String encodePropertyValues(Collection<String> values) {
        return String.join(PROPERTY_MULTI_VALUE_SEPARATOR, values.stream().toList());
    }

    public static Collection<String> decodePropertyValues(String values) {
        String[] splits = values.split(PROPERTY_MULTI_VALUE_SEPARATOR);
        return Arrays.stream(splits).toList();
    }
}
