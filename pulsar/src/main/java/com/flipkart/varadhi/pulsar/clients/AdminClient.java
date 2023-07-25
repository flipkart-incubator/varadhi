package com.flipkart.varadhi.pulsar.clients;

import com.flipkart.varadhi.exceptions.VaradhiException;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class AdminClient {
    private final PulsarAdmin pulsarAdmin;

    public AdminClient(PulsarAdmin pulsarAdmin) {
        this.pulsarAdmin = pulsarAdmin;
    }

    public void create(PulsarStorageTopic topic) {
        try {
            Map<String, String> properties = new HashMap<>();
            //TODO::see properties are needed, if any.;
            pulsarAdmin.topics().createPartitionedTopic(topic.getName(), topic.getPartitionCount(), properties);
            log.info("Created the pulsar topic:{}", topic.getName());
        } catch (PulsarAdminException e) {
            throw new VaradhiException(e);
        }
    }
}
