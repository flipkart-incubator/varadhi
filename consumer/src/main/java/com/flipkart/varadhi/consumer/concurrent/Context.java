package com.flipkart.varadhi.consumer.concurrent;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class Context {
    private final EventExecutor executor;
}
