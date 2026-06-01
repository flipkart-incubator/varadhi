package com.flipkart.varadhi.spi.db;

import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.failover.FailoverTransitionObject;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverOperation;

/**
 * Multi-write atomic operations needed by the topic-failover executor.
 *
 * <p>Implemented by the metastore backend (currently {@code metastore-zk}). The
 * controller module depends only on this SPI to keep its dependency on the storage
 * layer one-way (through {@code spi}, never directly on {@code metastore-zk}).
 *
 * <p>Each method below corresponds to one stage transition in the failover flow:
 * <ol>
 *   <li>{@link #createFailoverWithOp} – Phase 0: create FTO + Op atomically.</li>
 *   <li>{@link #commitSwitch}         – SWITCH:  Op-state + Topic-snapshot atomically;
 *       Topic write is <b>tracked</b> (emits an L1 event) so pods see the new state.</li>
 *   <li>{@link #commitSuccess}        – COMPLETED: Op (untracked), Topic source ->
 *       Replicating (untracked), and FTO delete in one txn.</li>
 *   <li>{@link #commitFailure}        – ABORTED:  Op (untracked) + FTO delete in one txn.
 *       Leaves the Topic snapshot untouched.</li>
 * </ol>
 */
public interface TopicFailoverTransactions {

    void createFailoverWithOp(TopicFailoverOperation op, FailoverTransitionObject fto);

    /**
     * Persist the in-memory Op and Topic state in one atomic ZK multi-txn.
     * On return both entities have their post-txn version stamped on the in-memory
     * object so the caller can read the new {@code topic.getVersion()} to ship in the
     * {@code FailoverStageEvent}.
     */
    void commitSwitch(TopicFailoverOperation op, VaradhiTopic topicWithUpdatedStates);

    void commitSuccess(TopicFailoverOperation op, VaradhiTopic topicWithSourceReplicating, String topicFqn);

    void commitFailure(TopicFailoverOperation op, String topicFqn);
}
