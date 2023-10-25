package com.flipkart.varadhi.pulsar.clients;

import com.flipkart.varadhi.exceptions.MessagingException;
import com.flipkart.varadhi.pulsar.config.PulsarAdminOptions;
import com.flipkart.varadhi.pulsar.config.PulsarClientOptions;
import com.flipkart.varadhi.pulsar.config.PulsarConfig;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ClientProvider {
    private static final TimeUnit TimeUnitMillis = TimeUnit.MILLISECONDS;
    private PulsarClient pulsarClient;
    private PulsarAdmin adminClient;

    public ClientProvider(PulsarConfig pulsarConfig) {
        this.pulsarClient = buildPulsarClient(pulsarConfig.getPulsarClientOptions());
        this.adminClient = buildPulsarAdminClient(pulsarConfig.getPulsarAdminOptions());
    }

    //TODO::Add authentication to PulsarClient and PulsarAdmin. It should be optional however.
    private PulsarClient buildPulsarClient(PulsarClientOptions options) {
        try {
            Map<String, Object> clientConfig = options.asConfigMap();
            return PulsarClient.builder().loadConf(clientConfig).build();
        } catch (PulsarClientException e) {
            throw new MessagingException(String.format("Failed to create PulsarClient. Error: %s.", e.getMessage()), e);
        }
    }

    PulsarAdmin buildPulsarAdminClient(PulsarAdminOptions options) {
        try {
            return PulsarAdmin.builder()
                    .serviceHttpUrl(options.getServiceHttpUrl())
                    .connectionTimeout(options.getConnectionTimeoutMs(), TimeUnitMillis)
                    .requestTimeout(options.getRequestTimeoutMs(), TimeUnitMillis)
                    .readTimeout(options.getReadTimeoutMs(), TimeUnitMillis)
                    .build();
        } catch (PulsarClientException e) {
            throw new MessagingException(
                    String.format("Failed to create PulsarAdminClient. Error: %s.", e.getMessage()), e);
        }
    }

    public PulsarClient getPulsarClient() {
        return pulsarClient;
    }

    public PulsarAdmin getAdminClient() {
        return adminClient;
    }
}
