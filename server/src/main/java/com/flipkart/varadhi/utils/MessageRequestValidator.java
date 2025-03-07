package com.flipkart.varadhi.utils;

import java.util.Map;

import com.flipkart.varadhi.config.MessageConfiguration;
import com.flipkart.varadhi.entities.StdHeaders;
import com.google.common.base.Utf8;
import com.google.common.collect.Multimap;

public class MessageRequestValidator {

    /**
     * The headers must be normalized according to the header config. If the config is capitalized, then the headers
     * object here must be capitalized as well.
     * 
     * @param msgConfig
     * @param requestHeaders
     * @param bodyLength
     */
    public static void ensureHeaderSemanticsAndSize(
        MessageConfiguration msgConfig,
        Multimap<String, String> requestHeaders,
        long bodyLength
    ) {
        msgConfig.ensureRequiredHeaders(requestHeaders);

        validateIdHeader(msgConfig, StdHeaders.get().msgId(), requestHeaders);
        validateIdHeader(msgConfig, StdHeaders.get().groupId(), requestHeaders);

        long headersAndBodySize = 0;
        for (Map.Entry<String, String> entry : requestHeaders.entries()) {
            String key = entry.getKey();
            String value = entry.getValue();
            int byteLength = Utf8.encodedLength(key) + Utf8.encodedLength(value);
            headersAndBodySize += byteLength;
        }
        headersAndBodySize += bodyLength;

        // If the total size of the headerNames and body exceeds the allowed limit, throw an exception
        if (headersAndBodySize > msgConfig.getMaxRequestSize()) {
            throw new IllegalArgumentException(
                String.format("Request size exceeds allowed limit of %d bytes.", msgConfig.getMaxRequestSize())
            );
        }
    }

    private static void validateIdHeader(MessageConfiguration msgConfig, String key, Multimap<String, String> headers) {
        var value = headers.get(key);

        // google multimap returns empty collection for non existent key
        if (!value.isEmpty()) {
            if (value.size() > 1) {
                throw new IllegalArgumentException(String.format("Multiple values for %s header found.", key));
            }
            var first = value.iterator().next();
            if (first.length() > msgConfig.getMaxIdHeaderSize()) {
                throw new IllegalArgumentException(
                    String.format("%s %s exceeds allowed size of %d.", key, first, msgConfig.getMaxIdHeaderSize())
                );
            }
        }
    }
}
