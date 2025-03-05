package com.flipkart.varadhi.utils;

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
        msgConfig.ensureRequiredHeaders(requestHeaders);
        long headersAndBodySize = 0;

        for (Map.Entry<String, String> entry : requestHeaders.entries()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (isIdHeader(msgConfig.stdHeaders(), key)) {
                if (value.length() > msgConfig.maxHeaderValueSize()) {
                    throw new IllegalArgumentException(
                        String.format(
                            "%s %s exceeds allowed size of %d.",
                            key.equals(msgConfig.stdHeaders().msgId()) ? "Message id" : "Group id",
                            value,
                            msgConfig.maxHeaderValueSize()
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
}
