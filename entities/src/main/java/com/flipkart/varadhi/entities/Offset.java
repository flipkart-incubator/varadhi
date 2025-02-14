package com.flipkart.varadhi.entities;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;

/**
 * Offset of the message in the topic.
 * Details are messaging stack specific.
 */

@JsonTypeInfo (use = JsonTypeInfo.Id.NAME, property = "@offsetType")
public interface Offset extends Comparable<Offset>, Serializable {
}
