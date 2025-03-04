package com.flipkart.varadhi.entities;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

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
    private final String msgId;
    private final String groupId;
    private final String callbackCodes;
    private final String requestTimeout;
    private final String replyToHttpUri;
    private final String replyToHttpMethod;
    private final String replyTo;
    private final String httpUri;
    private final String httpMethod;
    private final String httpContentType;
    private final String produceIdentity;
    private final String produceRegion;
    private final String produceTimestamp;
    private final List<String> allHeaders;

    @JsonCreator
    public StdHeaders(
        @JsonProperty ("allowedPrefix") List<String> allowedPrefix,
        @JsonProperty ("msgId") String msgId,
        @JsonProperty ("groupId") String groupId,
        @JsonProperty ("callbackCodes") String callbackCodes,
        @JsonProperty ("requestTimeout") String requestTimeout,
        @JsonProperty ("replyToHttpUri") String replyToHttpUri,
        @JsonProperty ("replyToHttpMethod") String replyToHttpMethod,
        @JsonProperty ("replyTo") String replyTo,
        @JsonProperty ("httpUri") String httpUri,
        @JsonProperty ("httpMethod") String httpMethod,
        @JsonProperty ("httpContentType") String httpContentType,
        @JsonProperty ("produceIdentity") String produceIdentity,
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
        this.produceIdentity = produceIdentity;
        this.produceRegion = produceRegion;
        this.produceTimestamp = produceTimestamp;
        this.allHeaders = List.of(
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
            this.produceIdentity,
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

    public String msgId() {
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

    public String replyToHttpUri() {
        return this.replyToHttpUri;
    }

    public String replyToHttpMethod() {
        return this.replyToHttpMethod;
    }

    public String replyTo() {
        return this.replyTo;
    }

    public String httpUri() {
        return this.httpUri;
    }

    public String httpMethod() {
        return this.httpMethod;
    }

    public String httpContentType() {
        return this.httpContentType;
    }

    public String produceIdentity() {
        return this.produceIdentity;
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
}
