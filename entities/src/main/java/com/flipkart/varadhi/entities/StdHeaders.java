package com.flipkart.varadhi.entities;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
    private final HeaderSpec groupId;
    private final HeaderSpec callbackCodes;
    private final HeaderSpec requestTimeout;
    private final HeaderSpec replyToHttpUri;
    private final HeaderSpec replyToHttpMethod;
    private final HeaderSpec replyTo;
    private final HeaderSpec httpUri;
    private final HeaderSpec httpMethod;
    private final HeaderSpec httpContentType;
    private final HeaderSpec producerIdentity;
    private final HeaderSpec produceRegion;
    private final HeaderSpec produceTimestamp;
    /** All configured standard header specs, stable iteration order. */
    private final List<HeaderSpec> allHeaderSpecs;
    /** Uppercase header name strings derived from {@link #allHeaderSpecs} (for validation only). */
    private final List<String> allHeaders;
    /**
     * Immutable name lists derived once from {@link #allHeaderSpecs} (see
     * {@link ProduceRequiredHeaderNameLists#fromSpecs(List)}).
     */
    private final List<String> headerNamesRequiredForQueueProduce;
    private final List<String> headerNamesRequiredForProduce;

    @JsonCreator
    @VisibleForTesting
    public StdHeaders(
        @JsonProperty ("allowedPrefix") List<String> allowedPrefix,
        @JsonProperty ("msgId") HeaderSpec msgId,
        @JsonProperty ("groupId") HeaderSpec groupId,
        @JsonProperty ("callbackCodes") HeaderSpec callbackCodes,
        @JsonProperty ("requestTimeout") HeaderSpec requestTimeout,
        @JsonProperty ("replyToHttpUri") HeaderSpec replyToHttpUri,
        @JsonProperty ("replyToHttpMethod") HeaderSpec replyToHttpMethod,
        @JsonProperty ("replyTo") HeaderSpec replyTo,
        @JsonProperty ("httpUri") HeaderSpec httpUri,
        @JsonProperty ("httpMethod") HeaderSpec httpMethod,
        @JsonProperty ("httpContentType") HeaderSpec httpContentType,
        @JsonProperty ("producerIdentity") HeaderSpec producerIdentity,
        @JsonProperty ("produceRegion") HeaderSpec produceRegion,
        @JsonProperty ("produceTimestamp") HeaderSpec produceTimestamp
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
        this.allHeaderSpecs = List.of(
            this.msgId,
            this.groupId,
            this.callbackCodes,
            this.requestTimeout,
            this.replyToHttpUri,
            this.replyToHttpMethod,
            this.replyTo,
            this.httpUri,
            this.httpMethod,
            this.httpContentType,
            this.producerIdentity,
            this.produceRegion,
            this.produceTimestamp
        );
        this.allHeaders = this.allHeaderSpecs.stream().map(HeaderSpec::value).toList();
        ProduceRequiredHeaderNameLists produceLists = ProduceRequiredHeaderNameLists.fromSpecs(this.allHeaderSpecs);
        this.headerNamesRequiredForQueueProduce = produceLists.queueProduce();
        this.headerNamesRequiredForProduce = produceLists.standardProduce();
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

    public HeaderSpec groupIdSpec() {
        return this.groupId;
    }

    public String groupId() {
        return this.groupId.value();
    }

    public HeaderSpec callbackCodes() {
        return this.callbackCodes;
    }

    public HeaderSpec requestTimeout() {
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

    public HeaderSpec httpContentType() {
        return this.httpContentType;
    }

    public HeaderSpec producerIdentity() {
        return this.producerIdentity;
    }

    public HeaderSpec produceRegion() {
        return this.produceRegion;
    }

    public HeaderSpec produceTimestamp() {
        return this.produceTimestamp;
    }

    @JsonIgnore
    public List<String> getAllHeaderNames() {
        return allHeaders;
    }

    /**
     * Header names required when producing to a queue: every configured header whose {@link RequiredBy} is
     * {@link RequiredBy#Queue} or {@link RequiredBy#All} ({@link RequiredBy#isRequiredOnQueueProduce()}).
     */
    @JsonIgnore
    public List<String> getHeaderNamesRequiredForQueueProduce() {
        return headerNamesRequiredForQueueProduce;
    }

    /**
     * Header names required when producing to resource: every configured header whose {@link RequiredBy} is
     * {@link RequiredBy#All}
     */
    @JsonIgnore
    public List<String> getHeaderNamesRequiredForProduce() {
        return headerNamesRequiredForProduce;
    }

    /**
     * Single pass over header specs to build both required-name lists (stable {@link HeaderSpec#value()} order).
     */
    private record ProduceRequiredHeaderNameLists(List<String> queueProduce, List<String> standardProduce) {
        private static ProduceRequiredHeaderNameLists fromSpecs(List<HeaderSpec> specs) {
            List<String> queue = new ArrayList<>();
            List<String> standard = new ArrayList<>();
            for (HeaderSpec spec : specs) {
                RequiredBy rb = spec.requiredBy();
                if (rb.isRequiredOnQueueProduce()) {
                    queue.add(spec.value());
                }
                if (rb.isRequiredOnProduce()) {
                    standard.add(spec.value());
                }
            }
            return new ProduceRequiredHeaderNameLists(List.copyOf(queue), List.copyOf(standard));
        }
    }
}
