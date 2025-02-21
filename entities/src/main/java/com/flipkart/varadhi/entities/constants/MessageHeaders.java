package com.flipkart.varadhi.entities.constants;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum MessageHeaders {

    @JsonProperty ("msgIdHeader") MSG_ID,

    @JsonProperty ("groupIdHeader") GROUP_ID,

    @JsonProperty ("callbackCodes") CALLBACK_CODE,

    @JsonProperty ("requestTimeout") REQUEST_TIMEOUT,

    @JsonProperty ("replyToHttpUriHeader") REPLY_TO_HTTP_URI,

    @JsonProperty ("replyToHttpMethodHeader") REPLY_TO_HTTP_METHOD,

    @JsonProperty ("replyToHeader") REPLY_TO,

    @JsonProperty ("httpUriHeader") HTTP_URI,

    @JsonProperty ("httpMethodHeader") HTTP_METHOD,

    @JsonProperty ("httpContentType") CONTENT_TYPE,

    @JsonProperty ("produceIdentity") PRODUCE_IDENTITY,

    @JsonProperty ("produceRegion") PRODUCE_REGION,

    @JsonProperty ("produceTimestamp") PRODUCE_TIMESTAMP
}
