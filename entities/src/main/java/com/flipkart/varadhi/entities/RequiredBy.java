package com.flipkart.varadhi.entities;

/**
 * Indicates where a standard header is required: Subscription-only, Queue-only, both, or callback flows only.
 */
public enum RequiredBy {
    Subscription, Queue, Both, Callback
}
