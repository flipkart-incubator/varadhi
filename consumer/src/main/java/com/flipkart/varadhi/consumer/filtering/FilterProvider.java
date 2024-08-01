package com.flipkart.varadhi.consumer.filtering;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class FilterProvider {


    public static MessageFilter get() {
        return new MessageFilter();
    }
}
