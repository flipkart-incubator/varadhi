package com.flipkart.varadhi.utils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.vertx.core.MultiMap;

import static com.flipkart.varadhi.MessageConstants.Headers.VARADHI_HEADER_PREFIX;

public class HeaderUtils {
    public static Multimap<String, String> copyVaradhiHeaders(MultiMap headers) {
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        headers.entries().forEach(entry -> {
            if (entry.getKey().toLowerCase().startsWith(VARADHI_HEADER_PREFIX)) {
                varadhiHeaders.put(entry.getKey(), entry.getValue());
            }
        });
        return varadhiHeaders;
    }
}
