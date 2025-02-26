package com.flipkart.varadhi.web.v1.produce;

import com.flipkart.varadhi.entities.utils.HeaderUtils;
import com.flipkart.varadhi.entities.constants.MessageHeaders;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.ext.web.RoutingContext;
import lombok.AllArgsConstructor;

import java.util.*;

@AllArgsConstructor
public class PreProduceHandler implements Handler<RoutingContext> {

    /**
     * Pre produce handler to do validations before producing it as message to data store.
     *
     * @param ctx the routing context to handle
     */
    @Override
    public void handle(RoutingContext ctx) {
        long bodyLength = ctx.body().length();
        ensureHeaderSemanticsAndSize(ctx.request().headers(), bodyLength);
        ctx.next();
    }

    private void ensureHeaderSemanticsAndSize(MultiMap requestHeaders, long bodyLength) {
        HeaderUtils.ensureRequiredHeaders(requestHeaders);
        long headersAndBodySize = 0;

        for (Map.Entry<String, String> entry : requestHeaders.entries()) {
            String key = entry.getKey();
            String value = entry.getValue();

            if (isIdHeader(key)) {
                if (value.length() > HeaderUtils.headerValueSizeMax) {
                    throw new IllegalArgumentException(
                        String.format(
                            "%s %s exceeds allowed size of %d.",
                            key.equals(HeaderUtils.getHeader(MessageHeaders.MSG_ID)) ? "Message id" : "Group id",
                            value,
                            HeaderUtils.headerValueSizeMax
                        )
                    );
                }
            }
            // Calculate the size of each header and its value
            int byteLength = (key.length() + value.length()) * 2;
            headersAndBodySize += byteLength;
        }

        headersAndBodySize += bodyLength;

        // If the total size of the headers and body exceeds the allowed limit, throw an exception
        if (headersAndBodySize > HeaderUtils.maxRequestSize) {
            throw new IllegalArgumentException(
                String.format("Request size exceeds allowed limit of %d bytes.", HeaderUtils.maxRequestSize)
            );
        }
    }

    private boolean isIdHeader(String key) {
        return key.equals(HeaderUtils.getHeader(MessageHeaders.MSG_ID)) || key.equals(
            HeaderUtils.getHeader(MessageHeaders.GROUP_ID)
        );
    }
}
