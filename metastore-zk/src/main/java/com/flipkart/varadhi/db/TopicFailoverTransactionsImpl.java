package com.flipkart.varadhi.db;

import com.flipkart.varadhi.entities.MetaStoreEntityType;
import com.flipkart.varadhi.entities.VaradhiTopic;
import com.flipkart.varadhi.entities.cluster.failover.FailoverTransitionObject;
import com.flipkart.varadhi.entities.cluster.failover.TopicFailoverOperation;
import com.flipkart.varadhi.spi.db.TopicFailoverTransactions;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.api.transaction.CuratorOp;

import java.util.ArrayList;
import java.util.List;

/**
 * ZK-backed implementation of {@link TopicFailoverTransactions}. Delegates the
 * heavy lifting to {@link ZKMetaStore#multi}: this class just stitches together
 * the right mix of tracked / untracked ops for each stage.
 */
@Slf4j
public final class TopicFailoverTransactionsImpl implements TopicFailoverTransactions {

    private final ZKMetaStore zk;

    public TopicFailoverTransactionsImpl(ZKMetaStore zk) {
        this.zk = zk;
    }

    @Override
    public void createFailoverWithOp(TopicFailoverOperation op, FailoverTransitionObject fto) {
        List<CuratorOp> ops = List.of(
            zk.createOpWithData(ZNode.ofTopicFailoverOp(op.getName()), op),
            zk.createOpWithData(ZNode.ofTopicFailover(fto.getName()), fto)
        );
        zk.multi(ops, List.of());
    }

    @Override
    public void commitSwitch(TopicFailoverOperation op, VaradhiTopic topic) {
        List<CuratorOp> ops = new ArrayList<>();
        ops.add(zk.setDataOp(ZNode.ofTopicFailoverOp(op.getName()), op));
        ops.addAll(zk.setDataOpTracked(ZNode.ofTopic(topic.getName()), topic, MetaStoreEntityType.TOPIC));
        zk.multi(ops, List.of(op, topic));
    }

    @Override
    public void commitSuccess(TopicFailoverOperation op, VaradhiTopic topic, String topicFqn) {
        List<CuratorOp> ops = new ArrayList<>();
        if (topic != null) {
            ops.add(zk.setDataOp(ZNode.ofTopic(topic.getName()), topic));
        }
        ops.add(zk.setDataOp(ZNode.ofTopicFailoverOp(op.getName()), op));
        if (zk.zkPathExist(ZNode.ofTopicFailover(topicFqn))) {
            ops.add(zk.deleteOp(ZNode.ofTopicFailover(topicFqn)));
        }
        zk.multi(ops, topic == null ? List.of(op) : List.of(op, topic));
    }

    @Override
    public void commitFailure(TopicFailoverOperation op, String topicFqn) {
        List<CuratorOp> ops = new ArrayList<>();
        ops.add(zk.setDataOp(ZNode.ofTopicFailoverOp(op.getName()), op));
        if (zk.zkPathExist(ZNode.ofTopicFailover(topicFqn))) {
            ops.add(zk.deleteOp(ZNode.ofTopicFailover(topicFqn)));
        }
        zk.multi(ops, List.of(op));
    }
}
