package com.flipkart.varadhi.produce.config;

import com.flipkart.varadhi.produce.ProducerErrorMapper;
import com.flipkart.varadhi.produce.otel.ProducerMetricsEmitterImpl;
import lombok.Getter;

/**
 * Enumeration of error types that can occur during message production.
 * These error types are used for metrics collection and error handling.
 * The error types are categorized by the layer where they occur.
 *
 * @see ProducerMetricsEmitterImpl
 * @see ProducerErrorMapper
 */
@Getter
public enum ProducerErrorType {
    // API Layer Errors
    INVALID("invalid"),           // Validation failures
    AUTH("auth"),                 // Authentication/Authorization failures
    SERIALIZE("serialize"),       // Message serialization issues
    RATE_LIMIT("rate_limit"),     // Rate limiting errors

    // Service Layer Errors
    TOPIC_NOT_FOUND("not_found"), // Topic not found
    TOPIC_INACTIVE("inactive"),   // Topic not in active state
    PRODUCER_INIT("init"),        // Producer initialization failures
    CACHE("cache"),               // Cache-related errors

    // Storage Layer Errors
    CONN("conn"),                 // Connection issues
    TIMEOUT("timeout"),           // Write timeouts
    QUOTA("quota"),               // Quota exceeded
    STORAGE("storage"),           // Generic storage errors

    // Generic Error
    INTERNAL("internal");         // Unexpected internal errors

    private final String value;

    /**
     * Constructs a ProducerErrorType with the specified metric value.
     *
     * @param value The string value used in metrics reporting
     */
    ProducerErrorType(String value) {
        this.value = value;
    }
}
