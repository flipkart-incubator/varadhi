package com.flipkart.varadhi.entities.cluster.failover;

/**
 * The kind of transition a {@code TransitionObject} represents. Kept generic so the same
 * master-state machinery can be reused for future transitions; for {@link #FAILOVER} the only
 * functional effect is flipping the per-region produce gate.
 */
public enum TransitionKind {
    FAILOVER, STORAGE
}
