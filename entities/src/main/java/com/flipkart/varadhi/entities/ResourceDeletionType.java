package com.flipkart.varadhi.entities;

/**
 * Enum representing the types of resource deletion.
 */
public enum ResourceDeletionType {
    SOFT_DELETE,
    HARD_DELETE,
    DEFAULT;

    /**
     * Converts a string value to a ResourceDeletionType.
     *
     * @param value the string value to convert
     *
     * @return the corresponding ResourceDeletionType, or DEFAULT if the value is null, blank, or unrecognized
     */
    public static ResourceDeletionType fromValue(String value) {
        if (value == null || value.isBlank()) {
            return DEFAULT;
        }
        try {
            return valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return DEFAULT;
        }
    }
}
