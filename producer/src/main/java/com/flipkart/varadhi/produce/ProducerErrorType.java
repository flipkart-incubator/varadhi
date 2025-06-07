package com.flipkart.varadhi.produce;

import com.flipkart.varadhi.produce.telemetry.ProducerMetricsRecorderImpl;
import lombok.Getter;

/**
 * Enumeration of error types that can occur during message production.
 * These error types are used for metrics collection and error handling.
 * The error types are categorized by the layer where they occur.
 *
 * @see ProducerMetricsRecorderImpl
 * @see ProducerErrorMapper
 */
@Getter
public enum ProducerErrorType {
    // API Layer Errors
    INVALID("invalid"),           // Validation failures
    SERIALIZE("serialize"),       // Message serialization issues
    RATE_LIMIT("rate_limit"),     // Rate limiting errors

    // Service Layer Errors
    TOPIC_NOT_FOUND("not_found"), // Topic not found
    TOPIC_INACTIVE("inactive"),   // Topic not in active state
    INIT("init"),                 // Producer initialization failures

    // Storage Layer Errors
    CONNECTION("connection"),     // Connection issues
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
