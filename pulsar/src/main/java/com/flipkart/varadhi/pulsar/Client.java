package com.flipkart.varadhi.pulsar;

import com.flipkart.varadhi.exceptions.VaradhiException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Client {
    PulsarAdmin admin;

    //TODO::Should come from the config.
    String pulsarUrl = "http://localhost:8080";
    int connectTimeout = 2000;
    int readTimeout = 2000;
    int requestTimeout = 2000;
    TimeUnit tu = TimeUnit.MILLISECONDS;

    //TODO::tenant and namespace should be assigned when creating PulsarStorageTopic.
    String tenant = "public";
    String namespace = "default";
    String schema = "persistent";

    public void init() {
        try {
            //TODO::Add authn to the pulsar clients. It should be optional however.
            this.admin = PulsarAdmin.builder()
                    .serviceHttpUrl(pulsarUrl)
                    .connectionTimeout(connectTimeout, tu)
                    .requestTimeout(requestTimeout, tu)
                    .readTimeout(readTimeout, tu)
                    .build();
        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }
    }

    public void create(PulsarStorageTopic topic) {
        try {
            Map<String, String> properties = new HashMap<>();
            //TODO::see properties are needed, if any.
            String topicFqdn = getTopicFqdn(topic);
            admin.topics().createPartitionedTopic(topicFqdn, topic.getPartitionCount(), properties);
            log.info("Created the pulsar topic:{}", topicFqdn);
        } catch (PulsarAdminException e) {
            throw new VaradhiException(e);
        }
    }

    private String getTopicFqdn(PulsarStorageTopic topic) {
        return String.format("%s://%s/%s/%s", schema, tenant, namespace, topic.getName());
    }
}
