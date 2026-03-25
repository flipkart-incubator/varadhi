package com.flipkart.varadhi.entities;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;

public class StdHeaders {

    private static StdHeaders globalInstance;

    @Getter
    private static boolean globalInstanceInitialized = false;

    public static synchronized void init(StdHeaders stdHeaders) {
        if (globalInstanceInitialized) {
            throw new IllegalStateException("Already initialized");
        }
        globalInstance = stdHeaders;
        globalInstanceInitialized = true;
    }

    public static StdHeaders get() {
        return globalInstance;
    }

    private final List<String> allowedPrefix;
    private final HeaderSpec msgId;
    private final String groupId;
    private final String callbackCodes;
    private final String requestTimeout;
    private final HeaderSpec replyToHttpUri;
    private final HeaderSpec replyToHttpMethod;
    private final HeaderSpec replyTo;
    private final HeaderSpec httpUri;
    private final HeaderSpec httpMethod;
    private final String httpContentType;
    private final String producerIdentity;
    private final String produceRegion;
    private final String produceTimestamp;
    private final List<String> allHeaders;

    @JsonCreator
    @VisibleForTesting
    public StdHeaders(
        @JsonProperty ("allowedPrefix") List<String> allowedPrefix,
        @JsonProperty ("msgId") HeaderSpec msgId,
        @JsonProperty ("groupId") String groupId,
        @JsonProperty ("callbackCodes") String callbackCodes,
        @JsonProperty ("requestTimeout") String requestTimeout,
        @JsonProperty ("replyToHttpUri") HeaderSpec replyToHttpUri,
        @JsonProperty ("replyToHttpMethod") HeaderSpec replyToHttpMethod,
        @JsonProperty ("replyTo") HeaderSpec replyTo,
        @JsonProperty ("httpUri") HeaderSpec httpUri,
        @JsonProperty ("httpMethod") HeaderSpec httpMethod,
        @JsonProperty ("httpContentType") String httpContentType,
        @JsonProperty ("producerIdentity") String producerIdentity,
        @JsonProperty ("produceRegion") String produceRegion,
        @JsonProperty ("produceTimestamp") String produceTimestamp
    ) {
        this.allowedPrefix = Collections.unmodifiableList(allowedPrefix);
        this.msgId = msgId;
        this.groupId = groupId;
        this.callbackCodes = callbackCodes;
        this.requestTimeout = requestTimeout;
        this.replyToHttpUri = replyToHttpUri;
        this.replyToHttpMethod = replyToHttpMethod;
        this.replyTo = replyTo;
        this.httpUri = httpUri;
        this.httpMethod = httpMethod;
        this.httpContentType = httpContentType;
        this.producerIdentity = producerIdentity;
        this.produceRegion = produceRegion;
        this.produceTimestamp = produceTimestamp;
        this.allHeaders = List.of(
            this.msgId.value(),
            this.groupId,
            this.callbackCodes,
            this.requestTimeout,
            this.replyToHttpUri.value(),
            this.replyToHttpMethod.value(),
            this.replyTo.value(),
            this.httpUri.value(),
            this.httpMethod.value(),
            this.httpContentType,
            this.producerIdentity,
            this.produceRegion,
            this.produceTimestamp
        );
    }

    /*
        All the Getters. But not prefixing with "get" as it is not a bean class. Without get they look more idiomatic
        during the usage and appears like a field access.
        Not making the fields public as we don't want to expose the fields directly and want to reserve the flexibility
        to tweak the getters behaviour.
     */

    public List<String> allowedPrefix() {
        return this.allowedPrefix;
    }

    /**
     * Header name for message ID (for use in required-headers and request handling).
     */
    public String msgId() {
        return this.msgId.value();
    }

    /**
     * Full spec for message-ID header (value + requiredBy).
     */
    public HeaderSpec msgIdSpec() {
        return this.msgId;
    }

    public String groupId() {
        return this.groupId;
    }

    public String callbackCodes() {
        return this.callbackCodes;
    }

    public String requestTimeout() {
        return this.requestTimeout;
    }

    public HeaderSpec replyToHttpUri() {
        return this.replyToHttpUri;
    }

    public HeaderSpec replyToHttpMethod() {
        return this.replyToHttpMethod;
    }

    public HeaderSpec replyTo() {
        return this.replyTo;
    }

    public HeaderSpec httpUri() {
        return this.httpUri;
    }

    public HeaderSpec httpMethod() {
        return this.httpMethod;
    }

    public String httpContentType() {
        return this.httpContentType;
    }

    public String producerIdentity() {
        return this.producerIdentity;
    }

    public String produceRegion() {
        return this.produceRegion;
    }

    public String produceTimestamp() {
        return this.produceTimestamp;
    }

    @JsonIgnore
    public List<String> getAllHeaderNames() {
        return allHeaders;
    }

    /**
     * Header names for {@link HeaderSpec} entries required on queue produce ({@link RequiredBy#Queue} or
     * {@link RequiredBy#Both}).
     */
    @JsonIgnore
    public List<String> getHeaderNamesRequiredForQueueProduce() {
        return Stream.of(msgIdSpec(), replyToHttpUri(), replyToHttpMethod(), replyTo(), httpUri(), httpMethod())
                     .filter(spec -> {
                         RequiredBy r = spec.requiredBy();
                         return r == RequiredBy.Queue || r == RequiredBy.Both;
                     })
                     .map(HeaderSpec::value)
                     .toList();
    }
}
