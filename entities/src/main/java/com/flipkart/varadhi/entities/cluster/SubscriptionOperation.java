package com.flipkart.varadhi.entities.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.flipkart.varadhi.entities.UnsidelineRequest;
import com.flipkart.varadhi.entities.Versioned;
import lombok.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.flipkart.varadhi.entities.cluster.Operation.State.*;

@Getter
@EqualsAndHashCode (callSuper = true)
public class SubscriptionOperation extends Versioned implements OrderedOperation {
    private final String requestedBy;
    private final long startTime;
    private final OpData data;
    private final int retryAttempt;
    private final List<OpResult> results;
    private long endTime;


    @JsonCreator
    SubscriptionOperation(
        String operationId,
        int version,
        String requestedBy,
        long startTime,
        long endTime,
        OpData data,
        int retryAttempt,
        List<OpResult> results
    ) {
        super(operationId, version);
        this.requestedBy = requestedBy;
        this.startTime = startTime;
        this.endTime = endTime;
        this.data = data;
        this.retryAttempt = retryAttempt;
        this.results = results;
    }

    SubscriptionOperation(OpData data, String requestedBy) {
        super(data.operationId, 0);
        this.requestedBy = requestedBy;
        this.startTime = System.currentTimeMillis();
        this.endTime = 0;
        this.retryAttempt = 0;
        this.data = data;
        this.results = new ArrayList<>();
        this.results.add(OpResult.of(0));
    }

    public static SubscriptionOperation startOp(String subscriptionId, String requestedBy) {
        return new SubscriptionOperation(new StartData(subscriptionId), requestedBy);
    }

    public static SubscriptionOperation stopOp(String subscriptionId, String requestedBy) {
        return new SubscriptionOperation(new StopData(subscriptionId), requestedBy);
    }

    public static SubscriptionOperation reAssignShardOp(Assignment assignment, String requestedBy) {
        return new SubscriptionOperation(new ReassignShardData(assignment), requestedBy);
    }

    public static SubscriptionOperation unsidelineOp(
        String subscriptionId,
        UnsidelineRequest request,
        String requestedBy
    ) {
        return new SubscriptionOperation(new UnsidelineData(subscriptionId, request), requestedBy);
    }

    @Override
    public SubscriptionOperation nextRetry() {
        int attempt = retryAttempt + 1;
        results.add(0, OpResult.of(attempt));
        return new SubscriptionOperation(
            getId(),
            getVersion(),
            requestedBy,
            startTime,
            endTime,
            data,
            attempt,
            results
        );
    }

    @JsonIgnore
    @Override
    public String getId() {
        return data.getOperationId();
    }

    @JsonIgnore
    @Override
    public State getState() {
        return results.get(0).state;
    }

    @JsonIgnore
    @Override
    public String getErrorMsg() {
        return results.get(0).errorMsg;
    }

    @JsonIgnore
    @Override
    public String getOrderingKey() {
        return "Sub_" + data.getSubscriptionId();
    }

    @JsonIgnore
    @Override
    public boolean isDone() {
        return results.get(0).isDone();
    }


    @Override
    public void markFail(String reason) {
        results.get(0).markFail(reason);
        endTime = System.currentTimeMillis();
    }

    @Override
    public void markCompleted() {
        results.get(0).markCompleted();
        endTime = System.currentTimeMillis();
    }

    @Override
    public boolean hasFailed() {
        return results.get(0).hasFailed();
    }

    @JsonIgnore
    public boolean isInRetry() {
        return retryAttempt > 0;
    }

    public void update(State opState, String opError) {
        results.get(0).update(opState, opError);
        if (isDone()) {
            endTime = System.currentTimeMillis();
        }
    }

    public void update(List<ShardOperation> shardOps) {
        // This assumes that caller passes the complete list of shardOps for the respective SubOp.
        StringBuilder sb = new StringBuilder();
        int completedCount = 0;
        for (ShardOperation shardOp : shardOps) {
            ShardOperation.OpData opData = shardOp.getOpData();
            if (shardOp.hasFailed()) {
                if (!sb.isEmpty()) {
                    sb.append(", ");
                }
                sb.append(String.format("Shard:%d failed:%s", opData.getShardId(), shardOp.getErrorMsg()));
            }
            if (shardOp.isDone()) {
                completedCount++;
            }
        }

        if (completedCount == shardOps.size()) {
            if (sb.isEmpty()) {
                markCompleted();
            } else {
                markFail(sb.toString());
            }
        }
    }

    @Override
    public String toString() {
        return String.format(
            "{data=%s retryAttempt=%d, requestedBy=%s, startTime=%d, endTime=%d}",
            data,
            retryAttempt,
            requestedBy,
            startTime,
            endTime
        );
    }

    @Getter
    static class OpResult {
        private final long startTime;
        private final int retryAttempt;
        private State state;
        private String errorMsg;
        private long endTime;

        OpResult(long startTime, int retryAttempt, State state, String errorMsg, long endTime) {
            this.startTime = startTime;
            this.retryAttempt = retryAttempt;
            this.state = state;
            this.errorMsg = errorMsg;
            this.endTime = endTime;
        }

        static OpResult of(int retryCount) {
            return new OpResult(System.currentTimeMillis(), retryCount, IN_PROGRESS, null, 0);
        }

        @JsonIgnore
        public boolean isDone() {
            return state == COMPLETED || state == ERRORED;
        }

        public void markFail(String reason) {
            state = ERRORED;
            errorMsg = reason;
            endTime = System.currentTimeMillis();
        }

        public void markCompleted() {
            state = COMPLETED;
            endTime = System.currentTimeMillis();
        }

        public boolean hasFailed() {
            return state == ERRORED;
        }

        public void update(State opState, String opError) {
            state = opState;
            errorMsg = opError;
            if (isDone()) {
                endTime = System.currentTimeMillis();
            }
        }
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @JsonTypeInfo (use = JsonTypeInfo.Id.NAME, property = "@opDataType")
    @JsonSubTypes ({
        @JsonSubTypes.Type (value = StartData.class, name = "startSubData"),
        @JsonSubTypes.Type (value = StopData.class, name = "stopSubData"),
        @JsonSubTypes.Type (value = ReassignShardData.class, name = "reassignShardData"),
        @JsonSubTypes.Type (value = UnsidelineData.class, name = "unsidelineData"),})
    public static class OpData {
        private String operationId;
        private String subscriptionId;

        @Override
        public String toString() {
            return String.format("Id=%s, subscriptionId=%s", operationId, subscriptionId);
        }
    }


    @Data
    @NoArgsConstructor
    @EqualsAndHashCode (callSuper = true)
    public static class StartData extends OpData {
        StartData(String subscriptionId) {
            super(UUID.randomUUID().toString(), subscriptionId);
        }

        @Override
        public String toString() {
            return String.format("Start.OpData{%s}", super.toString());
        }
    }


    @Data
    @NoArgsConstructor
    @EqualsAndHashCode (callSuper = true)
    public static class StopData extends OpData {
        StopData(String subscriptionId) {
            super(UUID.randomUUID().toString(), subscriptionId);
        }

        @Override
        public String toString() {
            return String.format("Stop.OpData{%s}", super.toString());
        }
    }


    @Data
    @NoArgsConstructor
    @EqualsAndHashCode (callSuper = true)
    public static class ReassignShardData extends OpData {
        private Assignment assignment;

        ReassignShardData(Assignment assignment) {
            super(UUID.randomUUID().toString(), assignment.getSubscriptionId());
            this.assignment = assignment;
        }

        @Override
        public String toString() {
            return String.format("ReassignShard.OpData{%s %s}", super.toString(), assignment);
        }
    }


    @Data
    @NoArgsConstructor
    @EqualsAndHashCode (callSuper = true)
    public static class UnsidelineData extends OpData {
        private UnsidelineRequest request;

        UnsidelineData(String subscriptionId, UnsidelineRequest request) {
            super(UUID.randomUUID().toString(), subscriptionId);
            this.request = request;
        }

        @Override
        public String toString() {
            return String.format("Unsideline.OpData{%s}", super.toString());
        }
    }
}
