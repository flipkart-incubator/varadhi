package com.flipkart.varadhi.entities;

import com.google.common.collect.Multimap;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Headers {
    Multimap<String, String> validatedHeaders;
}
