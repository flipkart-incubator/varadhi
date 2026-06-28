package com.flipkart.varadhi.core.cluster.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionObject;

import java.util.List;

/** Response wrapper for the active-failovers listing (lets it travel as a single response object). */
public record ActiveFailovers(List<TransitionObject> transitions) {
}
