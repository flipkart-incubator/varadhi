package com.flipkart.varadhi.common.utils;

import java.util.List;
import java.util.Map;

import com.flipkart.varadhi.config.MessageConfiguration;
import com.flipkart.varadhi.entities.StdHeaders;
import com.google.common.base.Utf8;
import com.google.common.collect.Multimap;

public class MessageRequestValidator {

    public static void ensureHeaderSemanticsAndSize(
        MessageConfiguration msgConfig,
        Multimap<String, String> requestHeaders,
        long bodyLength
    ) {
        ensureRequiredHeaders(requestHeaders);
        long headersAndBodySize = 0;

        for (Map.Entry<String, String> entry : requestHeaders.entries()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (isIdHeader(msgConfig.stdHeaders(), key)) {
                if (value.length() > msgConfig.maxHeaderIdSize()) {
                    throw new IllegalArgumentException(
                        String.format(
                            "%s %s exceeds allowed size of %d.",
                            key.equals(msgConfig.stdHeaders().msgId()) ? "Message id" : "Group id",
                            value,
                            msgConfig.maxHeaderIdSize()
                        )
                    );
                }
            }
            int byteLength = Utf8.encodedLength(key) + Utf8.encodedLength(value);
            headersAndBodySize += byteLength;
        }

        headersAndBodySize += bodyLength;

        // If the total size of the headerNames and body exceeds the allowed limit, throw an exception
        if (headersAndBodySize > msgConfig.maxRequestSize()) {
            throw new IllegalArgumentException(
                String.format("Request size exceeds allowed limit of %d bytes.", msgConfig.maxRequestSize())
            );
        }
    }

    private static boolean isIdHeader(StdHeaders stdHeaders, String key) {
        return key.equals(stdHeaders.msgId()) || key.equals(stdHeaders.groupId());
    }

    //might be separate for Topic/Queue in future
    public static List<String> getRequiredHeaders() {
        return List.of(StdHeaders.get().msgId());
    }

    public static void ensureRequiredHeaders(Multimap<String, String> headers) {
        getRequiredHeaders().forEach(key -> {
            if (!headers.containsKey(key)) {
                throw new IllegalArgumentException(String.format("Missing required header %s", key));
            }
        });
    }
}
