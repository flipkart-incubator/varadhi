package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.spi.services.ConsumerFactory;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.net.http.HttpClient;

@RequiredArgsConstructor
@Getter
public class ConsumerEnvironment {
    private final ProducerFactory producerFactory;
    private final ConsumerFactory consumerFactory;
    private final HttpClient httpClient;
    private final MeterRegistry meterRegistry;
}
