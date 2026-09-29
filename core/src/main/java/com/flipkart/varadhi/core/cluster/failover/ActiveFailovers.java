package com.flipkart.varadhi.core.cluster.failover;

import com.flipkart.varadhi.entities.cluster.failover.TransitionMaster;

import java.util.List;

/**
 * Response wrapper for the active-failovers listing. The cluster-bus {@code ResponseMessage} needs
 * a single concrete response type to serialize/deserialize against (a typed {@code getResponse()}),
 * so a raw {@code List<TransitionMaster>} cannot be returned directly — this wraps it in one.
 */
public record ActiveFailovers(List<TransitionMaster> transitions) {
}
