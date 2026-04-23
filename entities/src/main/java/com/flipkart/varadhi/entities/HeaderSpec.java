package com.flipkart.varadhi.entities;

/**
 * Spec for a standard header: header name value and queue-produce classification ({@link RequiredBy}).
 * in Java).
 */
public record HeaderSpec(String value, RequiredBy requiredBy) {
}
