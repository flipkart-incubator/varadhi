package com.flipkart.varadhi.utils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.vertx.core.MultiMap;

import java.util.List;

public class HeaderUtils {
    public static Multimap<String, String> copyVaradhiHeaders(MultiMap headers, List<String> allowedPrefix) {
        Multimap<String, String> varadhiHeaders = ArrayListMultimap.create();
        headers.entries().forEach(entry -> {
            String key = entry.getKey();
            boolean validPrefix = allowedPrefix.stream().anyMatch(key::startsWith);
            if (validPrefix) {
                varadhiHeaders.put(key, entry.getValue());
            }
        });
        return varadhiHeaders;
    }
}
