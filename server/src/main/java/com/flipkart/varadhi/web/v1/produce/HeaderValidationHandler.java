package com.flipkart.varadhi.web.v1.produce;

import com.flipkart.varadhi.config.VaradhiOptions;
import com.flipkart.varadhi.exceptions.ArgumentException;
import io.vertx.ext.web.RoutingContext;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.flipkart.varadhi.MessageConstants.Headers.VARADHI_HEADER_PREFIX;

public class HeaderValidationHandler {
    private int headerNameSizeMax;
    private int headerValueSizeMax;
    private int headersAllowedMax;

    public HeaderValidationHandler(VaradhiOptions varadhiOptions) {
        this.headerNameSizeMax = varadhiOptions.getHeaderNameSizeMax();
        this.headerValueSizeMax = varadhiOptions.getHeaderValueSizeMax();
        this.headersAllowedMax = varadhiOptions.getHeadersAllowedMax();
    }

    public void validate(RoutingContext ctx) {
        Set<String> headers = new HashSet<>();
        ctx.request().headers().entries().forEach((entry) -> {
            String key = entry.getKey().toLowerCase();
            if (key.startsWith(VARADHI_HEADER_PREFIX)) {
                validateEntry(entry);
                headers.add(key); // multi-value headers are considered one.
                if (headers.size() > headersAllowedMax) {
                    throw new ArgumentException(
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
            throw new ArgumentException(String.format("Header name %s exceeds allowed size.", entry.getKey()));
        }
        if (entry.getValue().length() > headerValueSizeMax) {
            throw new ArgumentException(String.format("Value of Header %s exceeds allowed size.", entry.getKey()));
        }
    }
}
