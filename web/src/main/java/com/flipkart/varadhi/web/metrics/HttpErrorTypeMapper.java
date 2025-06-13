package com.flipkart.varadhi.web.metrics;

/**
 * Maps HTTP status codes to standardized error type strings.
 * This provides consistent error type naming for metrics across the application.
 */
public final class HttpErrorTypeMapper {
    private HttpErrorTypeMapper() {
        // Prevent instantiation
        throw new AssertionError("Utility class should not be instantiated");
    }

    // Common HTTP error types
    public static final String BAD_REQUEST = "bad_request";
    public static final String UNAUTHORIZED = "unauthorized";
    public static final String FORBIDDEN = "forbidden";
    public static final String NOT_FOUND = "not_found";
    public static final String PAYLOAD_TOO_LARGE = "payload_too_large";
    public static final String TOO_MANY_REQUESTS = "too_many_requests";
    public static final String INTERNAL_SERVER_ERROR = "internal_server_error";
    public static final String SERVICE_UNAVAILABLE = "service_unavailable";
    public static final String UNKNOWN = "unknown";

    /**
     * Maps an HTTP status code to a standardized error type string.
     *
     * @param statusCode the HTTP status code
     * @return the corresponding error type string
     */
    public static String mapStatusCodeToErrorType(int statusCode) {
        return switch (statusCode) {
            case 400 -> BAD_REQUEST;
            case 401 -> UNAUTHORIZED;
            case 403 -> FORBIDDEN;
            case 404 -> NOT_FOUND;
            case 413 -> PAYLOAD_TOO_LARGE;
            case 429 -> TOO_MANY_REQUESTS;
            case 500 -> INTERNAL_SERVER_ERROR;
            case 503 -> SERVICE_UNAVAILABLE;
            default -> statusCode >= 400 ? "error_" + statusCode : UNKNOWN;
        };
    }
}
