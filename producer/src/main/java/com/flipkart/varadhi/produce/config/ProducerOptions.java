package com.flipkart.varadhi.produce.config;

import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Data;

/**
 * Configuration options for the producer component in the Varadhi messaging system.
 * This class encapsulates various settings that control producer behavior, caching,
 * and metrics collection.
 *
 * <p>The cache specifications follow the Guava CacheBuilderSpec format.
 * See {@link com.google.common.cache.CacheBuilderSpec} for more details.</p>
 *
 * @see com.google.common.cache.CacheBuilderSpec
 */
@Data
@Builder
public class ProducerOptions {

    /**
     * Cache specification for producer instances.
     * Follows Guava CacheBuilderSpec format.
     * Default: Expires after 1 hour of inactivity
     */
    @NotNull
    @Builder.Default
    private String producerCacheBuilderSpec = "expireAfterAccess=3600s";

    /**
     * Cache specification for topic metadata.
     * Follows Guava CacheBuilderSpec format.
     * Default: Expires after 1 hour of inactivity
     */
    @NotNull
    @Builder.Default
    private String topicCacheBuilderSpec = "expireAfterAccess=3600s";

    /**
     * Flag to enable or disable metric collection for producers.
     * When enabled, various metrics about producer performance and behavior will be collected.
     */
    @Builder.Default
    private boolean metricEnabled = false;

    /**
     * Configuration for producer metrics collection.
     * Only relevant when {@link #metricEnabled} is true.
     */
    @Builder.Default
    private ProducerMetricsConfig metricsConfig = ProducerMetricsConfig.getDefault();
}
