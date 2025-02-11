package com.flipkart.varadhi.web.v1.produce;

import com.flipkart.varadhi.config.MessageHeaderConfiguration;
import com.flipkart.varadhi.utils.HeaderUtils;
import com.google.common.collect.Multimap;
import io.vertx.ext.web.RoutingContext;
import lombok.AllArgsConstructor;

import java.nio.charset.StandardCharsets;
import java.util.*;

@AllArgsConstructor
public class HeaderValidationHandler {
    private MessageHeaderConfiguration messageHeaderConfiguration;
    private final String produceRegion;
    private static final int MAX_ID_LIMIT = 100;
    public static final String VALIDATED_HEADERS = "validatedHeaders";
    private static final String ANONYMOUS_IDENTITY = "Anonymous";
    public void validate(RoutingContext ctx) {
        Multimap<String, String> headers = HeaderUtils.copyVaradhiHeaders(ctx.request().headers(), messageHeaderConfiguration.getAllowedPrefix());
        String produceIdentity = ctx.user() == null ? ANONYMOUS_IDENTITY : ctx.user().subject();
        headers.put(messageHeaderConfiguration.getProduceRegion(), produceRegion);
        headers.put(messageHeaderConfiguration.getProduceIdentity(), produceIdentity);
        headers.put(messageHeaderConfiguration.getProduceTimestamp(), Long.toString(System.currentTimeMillis()));
        ensureHeaderSemanticsAndSize(headers, ctx.body().buffer().getBytes());
        ctx.put(VALIDATED_HEADERS, headers);
        ctx.next();
    }

    private void ensureHeaderSemanticsAndSize(Multimap<String, String> headers, byte[] body) {
        MessageHeaderConfiguration.ensureRequiredHeaders(messageHeaderConfiguration, headers);
        long headersAndBodySize = 0;

        for (Map.Entry<String, String> entry : headers.entries()) {
            String key = entry.getKey();
            String value = entry.getValue();

            // Check if the key matches either message ID or group ID header
            if (isIdHeader(key)) {
                if (value.length() > MAX_ID_LIMIT) {
                    throw new IllegalArgumentException(String.format("%s %s exceeds allowed size of %d.",
                            key.equals(messageHeaderConfiguration.getMsgIdHeader()) ? "Message id" : "Group id", value,
                            MAX_ID_LIMIT
                    ));
                }
            }

            // Calculate the size of each header and its value
            headersAndBodySize += key.getBytes(StandardCharsets.UTF_8).length + value.getBytes(StandardCharsets.UTF_8).length;
        }

        headersAndBodySize+= body.length;

        // If the total size of the headers and body exceeds the allowed limit, throw an exception
        if (headersAndBodySize > messageHeaderConfiguration.getMaxRequestSize()) {
            throw new IllegalArgumentException(String.format("Request size exceeds allowed limit of %d bytes.", messageHeaderConfiguration.getMaxRequestSize()));
        }
    }

    private boolean isIdHeader(String key) {
        return key.equals(messageHeaderConfiguration.getMsgIdHeader()) ||
                key.equals(messageHeaderConfiguration.getGroupIdHeader());
    }

}
