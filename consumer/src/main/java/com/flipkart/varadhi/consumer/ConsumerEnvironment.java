package com.flipkart.varadhi.consumer;

import com.flipkart.varadhi.entities.Offset;
import com.flipkart.varadhi.entities.StorageTopic;
import com.flipkart.varadhi.spi.services.ConsumerFactory;
import com.flipkart.varadhi.spi.services.ProducerFactory;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.net.http.HttpClient;

@RequiredArgsConstructor
@Getter
public class ConsumerEnvironment {
    private final ProducerFactory<StorageTopic> producerFactory;
    private final ConsumerFactory<StorageTopic, Offset> consumerFactory;
    private final HttpClient httpClient;
}
