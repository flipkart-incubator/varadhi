package com.flipkart.varadhi.pulsar.clients;

import com.flipkart.varadhi.exceptions.VaradhiException;
import com.flipkart.varadhi.pulsar.config.PulsarClientOptions;
import com.flipkart.varadhi.pulsar.entities.PulsarStorageTopic;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AdminClient {
    PulsarAdmin admin;
    TimeUnit tu = TimeUnit.MILLISECONDS;

    //TODO::tenant and namespace should be assigned when creating PulsarStorageTopic.
    String tenant = "public";
    String namespace = "default";
    String schema = "persistent";

    public AdminClient(PulsarClientOptions pulsarClientOptions) {
        try {
            //TODO::Add authentication to the pulsar clients. It should be optional however.
            this.admin = PulsarAdmin.builder()
                    .serviceHttpUrl(pulsarClientOptions.getPulsarUrl())
                    .connectionTimeout(pulsarClientOptions.getConnectTimeout(), tu)
                    .requestTimeout(pulsarClientOptions.getRequestTimeout(), tu)
                    .readTimeout(pulsarClientOptions.getReadTimeout(), tu)
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
