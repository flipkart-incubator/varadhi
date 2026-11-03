package com.flipkart.varadhi.entities.cluster.transition;

/**
 * The kind of transition represented by a transition operation. Kept generic so the same
 * transition-state machinery can be reused for future transitions; for {@link #FAILOVER} the only
 * functional effect is flipping the per-region produce gate.
 */
public enum TransitionKind {
    FAILOVER, STORAGE
}
