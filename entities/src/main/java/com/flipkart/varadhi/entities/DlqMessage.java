package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.flipkart.varadhi.entities.config.MessageHeaderConfiguration;
import com.flipkart.varadhi.entities.constants.StandardHeaders;
import com.flipkart.varadhi.entities.utils.HeadersDeserializer;
import com.flipkart.varadhi.entities.utils.HeadersSerializer;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class DlqMessage implements Message {
    private final byte[] payload;
    @JsonSerialize(using = HeadersSerializer.class)
    @JsonDeserialize(using = HeadersDeserializer.class)
    private final ArrayListMultimap<String, String> requestHeaders;
    private final Offset offset;
    private final int partitionIndex;

    @JsonIgnore
    @Override
    public String getMessageId() {
        return getHeader(StandardHeaders.msgIdHeader);
    }

    @JsonIgnore
    @Override
    public String getGroupId() {
        return getHeader(StandardHeaders.groupIdHeader);
    }

    @JsonIgnore
    @Override
    public boolean hasHeader(String key) {
        return requestHeaders.containsKey(key);
    }

    @JsonIgnore
    @Override
    public String getHeader(String key) {
        return !requestHeaders.containsKey(key) || requestHeaders.get(key).isEmpty() ? null :
                requestHeaders.get(key).get(0);
    }

    @JsonIgnore
    @Override
    public List<String> getHeaders(String key) {
        return new ArrayList<>(requestHeaders.get(key));
    }

    @JsonIgnore
    @Override
    public Multimap<String, String> getHeaders() {
        return requestHeaders;
    }

}
