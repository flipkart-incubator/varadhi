package com.flipkart.varadhi.produce;

import com.flipkart.varadhi.common.exceptions.ResourceNotFoundException;
import com.flipkart.varadhi.entities.TopicState;
import com.flipkart.varadhi.produce.config.ProducerErrorType;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * Maps various exceptions and topic states to standardized {@link ProducerErrorType} values.
 * This class provides centralized error mapping for the producer component of the messaging system.
 *
 * <p>The error mapping covers the following layers:
 * <ul>
 *   <li>API Layer - Validation, serialization, and authentication errors</li>
 *   <li>Service Layer - Topic and producer initialization errors</li>
 *   <li>Storage Layer - Connection, timeout, and quota errors</li>
 * </ul>
 *
 * @see ProducerErrorType
 */
public final class ProducerErrorMapper {

    private ProducerErrorMapper() {
        // Prevent instantiation
        throw new AssertionError("Utility class should not be instantiated");
    }

    /**
     * Maps exceptions to corresponding {@link ProducerErrorType} values.
     * This method handles various exceptions that can occur during message production
     * and maps them to standardized error types for consistent error handling and metrics.
     *
     * @param throwable the exception to map
     * @return the corresponding {@link ProducerErrorType}
     */
    public static ProducerErrorType mapToProducerErrorType(Throwable throwable) {
        if (throwable == null) {
            throw new IllegalArgumentException("Throwable cannot be null");
        }

        // Handle nested exceptions by getting the root cause
        Throwable rootCause = getRootCause(throwable);

        return switch (rootCause) {
            case ResourceNotFoundException ignored -> ProducerErrorType.TOPIC_NOT_FOUND;
            case PulsarClientException.TimeoutException ignored -> ProducerErrorType.TIMEOUT;
            case PulsarClientException.ProducerBusyException ignored -> ProducerErrorType.STORAGE;
            case PulsarClientException.ProducerQueueIsFullError ignored -> ProducerErrorType.QUOTA;
            case PulsarClientException.InvalidConfigurationException ignored -> ProducerErrorType.INVALID;
            case PulsarClientException.CryptoException ignored -> ProducerErrorType.SERIALIZE;
            case PulsarClientException.AlreadyClosedException ignored -> ProducerErrorType.PRODUCER_INIT;
            case PulsarClientException.TopicTerminatedException ignored -> ProducerErrorType.TOPIC_INACTIVE;
            case PulsarClientException.ConnectException ignored -> ProducerErrorType.CONN;
            case PulsarClientException.LookupException ignored -> ProducerErrorType.CONN;
            case PulsarClientException.NotConnectedException ignored -> ProducerErrorType.CONN;
            default -> ProducerErrorType.INTERNAL;
        };
    }

    /**
     * Maps topic states to corresponding {@link ProducerErrorType} values.
     * This method handles various topic states and maps them to standardized error types
     * for consistent error handling and metrics.
     *
     * @param state the {@link TopicState} to map
     * @return the corresponding {@link ProducerErrorType}
     * @throws IllegalArgumentException if state is null
     */
    public static ProducerErrorType mapTopicStateToErrorType(TopicState state) {
        if (state == null) {
            throw new IllegalArgumentException("TopicState cannot be null");
        }

        return switch (state) {
            case Replicating, Blocked -> ProducerErrorType.TOPIC_INACTIVE;
            case Throttled -> ProducerErrorType.RATE_LIMIT;
            default -> ProducerErrorType.INTERNAL;
        };
    }

    private static Throwable getRootCause(Throwable throwable) {
        Throwable cause = throwable;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }
}
