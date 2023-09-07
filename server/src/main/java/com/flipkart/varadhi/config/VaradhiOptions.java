package com.flipkart.varadhi.config;

import com.flipkart.varadhi.produce.config.ProducerOptions;
import lombok.Data;

import static com.flipkart.varadhi.Constants.REST_DEFAULTS.*;

@Data
public class VaradhiOptions {
    private String deployedRegion;
    private ProducerOptions producerOptions;
    private int payloadSizeMax = PAYLOAD_SIZE_MAX;
    private int headersAllowedMax = HEADERS_ALLOWED_MAX;
    private int headerNameSizeMax = HEADER_NAME_SIZE_MAX;
    private int headerValueSizeMax = HEADER_VALUE_SIZE_MAX;

}
