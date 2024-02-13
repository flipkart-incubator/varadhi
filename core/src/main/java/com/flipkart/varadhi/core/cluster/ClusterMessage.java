package com.flipkart.varadhi.core.cluster;


import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,  property = "@messageType")
@Data
public class ClusterMessage {
    String id;
}
