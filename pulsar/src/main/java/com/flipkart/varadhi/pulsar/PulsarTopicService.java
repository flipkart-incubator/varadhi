package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.services.StorageTopicService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PulsarTopicService extends StorageTopicService<PulsarStorageTopic> {

    Client client;

    public  PulsarTopicService(){
        client = new Client();
        client.init();
    }
    public void create(PulsarStorageTopic topic) {
        log.debug("Call Pulsar to create the required topic.");
        client.create(topic);
    }

}
