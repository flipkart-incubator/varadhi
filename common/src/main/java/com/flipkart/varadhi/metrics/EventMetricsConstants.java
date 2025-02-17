package com.flipkart.varadhi.metrics;

/**
 * Constants for Varadhi event metrics naming and tag conventions.
 * <p>
 * This class provides a centralized location for all event-related metric names
 * and tag keys. It follows the naming pattern:
 * {@code varadhi.event.<operation>.<metric_type>}
 * <p>
 * Example metric name: {@code varadhi.event.creation.latency}
 */
public final class EventMetricsConstants {
    /**
     * Base prefix for all event-related metrics.
     */
    public static final String METER_PREFIX = "varadhi.event.";

    private EventMetricsConstants() {
        throw new AssertionError("No instances allowed");
    }

    /**
     * Constants for event creation metrics.
     * <p>
     * These metrics track the creation of event markers in the system:
     * <ul>
     *     <li>Latency - Time taken to create events</li>
     *     <li>Total - Count of successful event creations</li>
     *     <li>Errors - Count of failed event creations</li>
     * </ul>
     */
    public static final class Creation {
        /**
         * Metric name for event creation latency (in milliseconds).
         * <p>
         * Tags: {@link Tags#RESOURCE_TYPE}
         */
        public static final String LATENCY = METER_PREFIX + "creation.latency";

        /**
         * Metric name for successful event creation count.
         * <p>
         * Tags: {@link Tags#RESOURCE_TYPE}
         */
        public static final String TOTAL = METER_PREFIX + "creation.total";

        /**
         * Metric name for failed event creation count.
         * <p>
         * Tags: {@link Tags#RESOURCE_TYPE}, {@link Tags#ERROR_TYPE}
         */
        public static final String ERRORS = METER_PREFIX + "creation.errors";

        private Creation() {
            throw new AssertionError("No instances allowed");
        }
    }


    /**
     * Constants for metric tag keys.
     * <p>
     * These tags are used to dimension the metrics for better analysis:
     * <ul>
     *     <li>resource_type - Type of resource the event is for</li>
     *     <li>error_type - Classification of error when operation fails</li>
     * </ul>
     */
    public static final class Tags {
        /**
         * Tag key for the type of resource (e.g., "PROJECT", "TOPIC").
         */
        public static final String RESOURCE_TYPE = "resource_type";

        /**
         * Tag key for the type of error encountered.
         */
        public static final String ERROR_TYPE = "error_type";

        private Tags() {
            throw new AssertionError("No instances allowed");
        }
    }
}
