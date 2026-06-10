package com.flipkart.varadhi.spi.services;

import java.util.concurrent.CompletableFuture;

/**
 * Pod-side SPI invoked during the failover {@code PREPARE} stage to pre-warm the
 * target-region broker before traffic is switched to it (open connections, lazily
 * initialize producer metadata, etc.). Pre-warming during PREPARE keeps the first
 * post-switch produce off the cold-start latency path.
 *
 * <p>The default {@link #NO_OP} does nothing and completes immediately, so failover
 * works without a messaging-stack-specific implementation; wire in a real warmer
 * (e.g. a Pulsar producer pre-create) where available.
 */
public interface BrokerWarmer {

    /**
     * Pre-warm producing to {@code topicFqn} in {@code targetRegion}.
     *
     * @return a future that completes when warming is done (or fails if it cannot warm)
     */
    CompletableFuture<Void> warm(String topicFqn, String targetRegion);

    BrokerWarmer NO_OP = (topicFqn, targetRegion) -> CompletableFuture.completedFuture(null);
}
