package com.flipkart.varadhi.spi.services;

import java.util.concurrent.CompletableFuture;

/**
 * SPI implemented per storage backend to "pre-warm" the target region's producer
 * before SWITCH. Invoked on each pod during the PREPARE stage so that when SWITCH
 * flips the {@code TopicState} the first produce request does not pay the
 * connection-open / metadata-fetch cost.
 *
 * <p>Failures here are non-fatal: the pod still acks PREPARE (so the controller
 * may proceed to SWITCH); a warm-up failure simply means the first produce after
 * SWITCH might be slower. Implementations should therefore swallow recoverable
 * errors and only complete exceptionally when the target is provably broken.
 */
public interface BrokerWarmer {

    /**
     * Open / pre-fetch producer connections for {@code topicFqn} in {@code targetRegion}.
     */
    CompletableFuture<Void> warm(String topicFqn, String targetRegion);

    /** Default no-op binding for tests and dev mode. */
    BrokerWarmer NO_OP = (topic, region) -> CompletableFuture.completedFuture(null);
}
