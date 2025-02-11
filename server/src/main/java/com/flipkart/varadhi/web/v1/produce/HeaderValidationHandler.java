package com.flipkart.varadhi.web.v1.produce;

import com.flipkart.varadhi.config.MessageHeaderConfiguration;
import com.flipkart.varadhi.entities.Message;
import com.flipkart.varadhi.entities.ProducerMessage;
import com.flipkart.varadhi.utils.HeaderUtils;
import com.google.common.collect.Multimap;
import io.vertx.ext.web.RoutingContext;
import lombok.AllArgsConstructor;

import java.util.*;

import static com.flipkart.varadhi.Constants.Tags.TAG_IDENTITY;
import static com.flipkart.varadhi.Constants.Tags.TAG_REGION;

@AllArgsConstructor
public class HeaderValidationHandler {
    private MessageHeaderConfiguration messageHeaderConfiguration;
    private final String produceRegion;
    private static final int MAX_ID_LIMIT = 100;
    public static final String VALIDATED_HEADERS = "validatedHeaders";
    private static final String ANONYMOUS_IDENTITY = "Anonymous";
    public void validate(RoutingContext ctx, MessageHeaderConfiguration config) {
        Multimap<String, String> headers = HeaderUtils.copyVaradhiHeaders(ctx.request().headers(), messageHeaderConfiguration.getAllowedPrefix());
        String produceIdentity = ctx.user() == null ? ANONYMOUS_IDENTITY : ctx.user().subject();
        headers.put(messageHeaderConfiguration.getProduceRegion(), produceRegion);
        headers.put(messageHeaderConfiguration.getProduceIdentity(), produceIdentity);
        headers.put(messageHeaderConfiguration.getProduceTimestamp(), Long.toString(System.currentTimeMillis()));
        validateHeaders(headers);
        ctx.put(VALIDATED_HEADERS, headers);
    }


    private void validateHeaders(Multimap<String, String> headers) {
        MessageHeaderConfiguration.ensureRequiredHeaders(messageHeaderConfiguration, headers);
        for (Map.Entry<String, String> entry : headers.entries()) {
            String key = entry.getKey().toLowerCase();
            String value = entry.getValue().toLowerCase();

            if (key.equals(messageHeaderConfiguration.getMsgIdHeader())) {
                if (value.length() > MAX_ID_LIMIT) {
                    throw new IllegalArgumentException(String.format("Message id %s exceeds allowed size of %d.", value, MAX_ID_LIMIT));
                }
            }

            if (key.equals(messageHeaderConfiguration.getGroupIdHeader())) {
                if (value.length() > MAX_ID_LIMIT) {
                    throw new IllegalArgumentException(String.format("Group id %s exceeds allowed size of %d.", value, MAX_ID_LIMIT));
                }
            }
        }
    }

}
