package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.cluster.failover.TransitionObject;

import java.util.List;

/**
 * Controller-only store for the master {@link TransitionObject} of an in-flight topic transition.
 * <p>
 * Writes here are <b>untracked</b> (no L1 fan-out): the object is never replicated to pods. The
 * store is keyed by {@code topicFqn} so {@link #create} doubles as the lock-free uniqueness guard
 * — a second concurrent failover for the same topic fails with a duplicate-resource error.
 */
public interface TransitionStore {

    /**
     * Atomically creates the transition. Fails with a duplicate-resource error if one already
     * exists for the topic (this is the "one active transition per topic" guard).
     */
    void create(TransitionObject transition);

    TransitionObject get(String topicFqn);

    boolean exists(String topicFqn);

    /** Optimistic (version-checked) update; conflicting concurrent updates fail and must re-read. */
    void update(TransitionObject transition);

    void delete(String topicFqn);

    /** All active transitions — used by the admin view and the recovery reconciler. */
    List<TransitionObject> listActive();
}
