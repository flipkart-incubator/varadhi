package com.flipkart.varadhi.web.v1.produce;

import com.flipkart.varadhi.config.RestOptions;
import io.vertx.ext.web.RoutingContext;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.flipkart.varadhi.entities.StandardHeaders.VARADHI_HEADER_PREFIX;

public class HeaderValidationHandler {
    private final int headerNameSizeMax;
    private final int headerValueSizeMax;
    private final int headersAllowedMax;

    public HeaderValidationHandler(RestOptions restOptions) {
        this.headerNameSizeMax = restOptions.getHeaderNameSizeMax();
        this.headerValueSizeMax = restOptions.getHeaderValueSizeMax();
        this.headersAllowedMax = restOptions.getHeadersAllowedMax();
    }

    public void validate(RoutingContext ctx) {
        Set<String> headers = new HashSet<>();
        ctx.request().headers().entries().forEach((entry) -> {
            String key = entry.getKey().toLowerCase();
            if (key.startsWith(VARADHI_HEADER_PREFIX)) {
                validateEntry(entry);
                headers.add(key); // multi-value headers are considered one.
                if (headers.size() >= headersAllowedMax) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "More Varadhi specific headers specified than allowed max(%d).",
                                    headersAllowedMax
                            ));
                }
            }
        });

        // TODO:: Discuss, shall ctx.next() be delegated at route setup (pre-handler setup)
        ctx.next();
    }

    private void validateEntry(Map.Entry<String, String> entry) {
        if (entry.getKey().length() > headerNameSizeMax) {
            throw new IllegalArgumentException(String.format("Header name %s exceeds allowed size.", entry.getKey()));
        }
        if (entry.getValue().length() > headerValueSizeMax) {
            throw new IllegalArgumentException(
                    String.format("Value of Header %s exceeds allowed size.", entry.getKey()));
        }
    }
}
