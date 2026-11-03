# Topic failover sequence

```mermaid
sequenceDiagram
    participant API as ControllerHandler<br/>Vert.x event-loop
    participant TS as TransitionService<br/>controller thread
    participant OM as OperationMgr<br/>operation executor
    participant EX as TopicFailoverOpExecutor<br/>topic-failover-stage-*<br/>fixed pool: 5
    participant SA as StageAwaiter<br/>failover-awaiter-*<br/>single scheduled thread
    participant ZK as VaradhiMetaStore / ZKMetaStore<br/>ZooKeeper client thread
    participant MEL as DefaultMetaStoreChangeListener<br/>metadata event callback
    participant REP as ResourceEventProcessor<br/>event-sender-* virtual threads
    participant CACHE as ResourceReadCache / TopicCache<br/>producer pod
    participant PTH as ProduceTransitionMsgHandler<br/>producer message-handler thread
    participant VW as Version-wait scheduler<br/>topic-transition-version-wait
    participant LAG as TopicFailoverOpExecutor<br/>topic-failover-lag-*<br/>fixed pool: 2

    API->>TS: enqueue(TopicFailoverOperation)
    TS->>OM: enqueueTopicFailover(...)
    OM->>EX: execute(operation)

    EX->>ZK: TopicStore.update(PREPARE)
    ZK-->>EX: Topic version N
    ZK->>ZK: Atomically create metadata event znode

    EX->>SA: expect(opId, PREPARE, hosts)
    EX->>REP: Broadcast PREPARE(version N)
    ZK-->>MEL: Topic metadata event
    MEL->>ZK: Read latest topic
    MEL->>REP: Publish ResourceEvent
    REP->>CACHE: Update topic cache to version N
    REP->>PTH: Deliver PREPARE event
    PTH->>VW: Wait for version N
    VW->>CACHE: Poll topic version
    CACHE-->>VW: Version N
    VW-->>PTH: Version converged
    PTH->>TS: TransitionAck(PREPARE)
    TS->>SA: recordAck(...)
    SA-->>EX: PREPARE barrier complete

    EX->>ZK: TopicStore.update(FENCE)
    ZK-->>EX: Topic version N+1
    ZK->>ZK: Atomically create metadata event znode
    EX->>SA: expect(opId, FENCE, hosts)
    EX->>REP: Broadcast FENCE(version N+1)
    ZK-->>MEL: Topic metadata event
    MEL->>REP: Publish ResourceEvent
    REP->>CACHE: Update topic cache to version N+1
    REP->>PTH: Deliver FENCE event
    PTH->>VW: Wait for version N+1
    VW->>CACHE: Poll topic version
    CACHE-->>VW: Version N+1
    VW-->>PTH: Version converged
    PTH->>TS: TransitionAck(FENCE)
    TS->>SA: recordAck(...)
    SA-->>EX: FENCE barrier complete

    EX->>LAG: awaitLagCleared()
    LAG->>LAG: Poll replication lag
    LAG-->>EX: Lag cleared

    EX->>ZK: TopicStore.update(MIGRATE)
    ZK-->>EX: Topic version N+2
    ZK->>ZK: Atomically create metadata event znode
    EX->>SA: expect(opId, MIGRATE, hosts)
    EX->>REP: Broadcast MIGRATE(version N+2)
    ZK-->>MEL: Topic metadata event
    MEL->>REP: Publish ResourceEvent
    REP->>CACHE: Update topic cache to version N+2
    REP->>PTH: Deliver MIGRATE event
    PTH->>VW: Wait for version N+2
    VW->>CACHE: Poll topic version
    CACHE-->>VW: Version N+2
    VW-->>PTH: Version converged
    PTH->>TS: TransitionAck(MIGRATE)
    TS->>SA: recordAck(...)
    SA-->>EX: MIGRATE barrier complete

    EX->>ZK: Mark operation COMPLETED
```

`LocalTopicCachePublisher` is not used. Cache propagation follows:

```text
VaradhiMetaStore
→ ZKMetaStore event znode
→ DefaultMetaStoreChangeListener
→ ResourceEventProcessor
→ producer TopicCache
```

The version-wait scheduler waits for cache convergence; it does not update the cache.
