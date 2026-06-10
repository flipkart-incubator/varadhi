package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.failover.FailoverTransitionObject;

import java.util.List;
import java.util.Optional;

public interface TopicStore {
    void create(VaradhiTopic topic);

    VaradhiTopic get(String topicName);

    List<String> getAllNames(String projectName);

    List<VaradhiTopic> getAll();

    boolean exists(String topicName);

    void update(VaradhiTopic topic);

    void delete(String topicName);

    /* ============== Failover Transition Object (pointer/L2) ============== */

    /**
     * Creates the failover pointer for {@code topicFqn}. Per-topic uniqueness is enforced
     * by ZK path creation; callers handle {@code DuplicateResourceException} as the
     * "failover already in progress" guard.
     *
     * <p>Untracked (no L1 event): pods don't see this entity.
     */
    void createFailover(FailoverTransitionObject fto);

    /** Reads the failover pointer for {@code topicFqn}, if any. */
    Optional<FailoverTransitionObject> getFailover(String topicFqn);

    /** {@code true} when {@code topicFqn} has a live failover pointer. Used as delete guard. */
    boolean hasFailover(String topicFqn);

    /** Deletes the failover pointer; no-op if absent. */
    void deleteFailover(String topicFqn);

    /** Lists every non-deleted FTO across all topics. Backs admin {@code /failovers/active}. */
    List<FailoverTransitionObject> getAllActiveFailovers();
}
