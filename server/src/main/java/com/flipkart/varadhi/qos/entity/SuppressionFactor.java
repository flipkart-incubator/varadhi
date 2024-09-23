package com.flipkart.varadhi.qos.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class SuppressionFactor {
    Float throughputFactor;
    Float qpsFactor;
}
