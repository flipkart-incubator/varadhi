package com.flipkart.varadhi.utils;

import com.flipkart.varadhi.entities.utils.HeaderUtils;
import com.google.common.base.Utf8;
import com.google.common.collect.Multimap;

import java.util.Map;

public class MessageRequestValidator {
    private MessageRequestValidator() {
    }

    public static void ensureHeaderSemanticsAndSize(Multimap<String, String> requestHeaders, long bodyLength) {
        HeaderUtils.ensureRequiredHeaders(requestHeaders);
        long headersAndBodySize = 0;

        for (Map.Entry<String, String> entry : requestHeaders.entries()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (isIdHeader(key)) {
                if (value.length() > HeaderUtils.getInstance().messageHeaderConfiguration.headerValueSizeMax()) {
                    throw new IllegalArgumentException(
                        String.format(
                            "%s %s exceeds allowed size of %d.",
                            key.equals(HeaderUtils.getInstance().messageHeaderConfiguration.getMsgIdHeaderKey()) ?
                                "Message id" :
                                "Group id",
                            value,
                            HeaderUtils.getInstance().messageHeaderConfiguration.headerValueSizeMax()
                        )
                    );
                }
            }
            int byteLength = Utf8.encodedLength(key) + Utf8.encodedLength(value);
            headersAndBodySize += byteLength;
        }

        headersAndBodySize += bodyLength;

        // If the total size of the headers and body exceeds the allowed limit, throw an exception
        if (headersAndBodySize > HeaderUtils.getInstance().messageHeaderConfiguration.maxRequestSize()) {
            throw new IllegalArgumentException(
                String.format(
                    "Request size exceeds allowed limit of %d bytes.",
                    HeaderUtils.getInstance().messageHeaderConfiguration.maxRequestSize()
                )
            );
        }
    }

    private static boolean isIdHeader(String key) {
        return key.equals(HeaderUtils.getInstance().messageHeaderConfiguration.getMsgIdHeaderKey()) || key.equals(
            HeaderUtils.getInstance().messageHeaderConfiguration.getGroupIdHeaderKey()
        );
    }
}
