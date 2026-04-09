package com.flipkart.varadhi.web;

import com.flipkart.varadhi.core.config.MessageConfiguration;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.StdHeaders;
import com.google.common.collect.Multimap;

public class MessageRequestValidator {

    /**
     * The headers must be normalized according to the header config. If the config is capitalized, then the headers
     * object here must be capitalized as well.
     * 
     * @param msgConfig
     */
    public static void ensureHeaderSemanticsAndSize(MessageConfiguration msgConfig, Message message) {
        msgConfig.ensureRequiredHeaders(message.getHeaders());
        validateIdHeadersAndRequestSize(msgConfig, message);
    }

    /**
     * Like {@link #ensureHeaderSemanticsAndSize} but requires every configured header whose
     * {@link com.flipkart.varadhi.entities.RequiredBy#isRequiredOnQueueProduce()} is true (queue produce).
     */
    public static void ensureHeaderSemanticsAndSizeForQueueProduce(MessageConfiguration msgConfig, Message message) {
        msgConfig.ensureQueueProduceRequiredHeaders(message.getHeaders());
        validateIdHeadersAndRequestSize(msgConfig, message);
    }

    private static void validateIdHeadersAndRequestSize(MessageConfiguration msgConfig, Message message) {
        validateIdHeader(msgConfig, StdHeaders.get().msgId(), message.getHeaders());
        validateIdHeader(msgConfig, StdHeaders.get().groupId(), message.getHeaders());

        int totalSizeBytes = message.getTotalSizeBytes();
        if (totalSizeBytes > msgConfig.getMaxRequestSize()) {
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
