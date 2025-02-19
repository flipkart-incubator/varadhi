package com.flipkart.varadhi.web.v1.produce;

import com.flipkart.varadhi.entities.config.MessageHeaderConfiguration;
import com.flipkart.varadhi.entities.constants.StandardHeaders;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.vertx.core.MultiMap;
import io.vertx.ext.web.RoutingContext;
import lombok.AllArgsConstructor;

import java.nio.charset.StandardCharsets;
import java.util.*;

@AllArgsConstructor
public class PreProduceHandler {
    //rename to generic
    private MessageHeaderConfiguration messageHeaderConfiguration;
    private static final int MAX_ID_LIMIT = 100;
    public void validate(RoutingContext ctx) {
        validateHeadersAndBodyForMessage(ctx);
        ctx.next();
    }

    public void validateHeadersAndBodyForMessage(RoutingContext ctx){
        ensureHeaderSemanticsAndSize(ctx.request().headers(), ctx.body().buffer().getBytes());
    }

    private void ensureHeaderSemanticsAndSize(MultiMap headers, byte[] body) {
        Multimap<String, String> requestHeaders = ArrayListMultimap.create();
        headers.entries().forEach(entry -> {
            String key = entry.getKey();
            requestHeaders.put(key, entry.getValue());
        });
        StandardHeaders.ensureRequiredHeaders(messageHeaderConfiguration, requestHeaders);
        long headersAndBodySize = 0;

        for (Map.Entry<String, String> entry : headers.entries()) {
            String key = entry.getKey();
            String value = entry.getValue();

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
