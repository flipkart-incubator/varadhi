# Varadhi Topic Model: Current State & Global Topics Proposal

## Overview

This document summarizes the current topic model structure and proposes changes to support Global Topics with multi-zone replication and failover capabilities.

---

## Current Topic Model Structure

### Class Hierarchy

```
AbstractTopic (interface)
    │
    │ Methods:
    │   - String getName()
    │
    ├─── StorageTopic (abstract class)
    │       │
    │       Fields:
    │       - int id
    │       - String name
    │       │
    │       └─── PulsarStorageTopic (concrete implementation)
    │               │
    │               Fields:
    │               - int partitionCount
    │
    └─── VaradhiTopic (concrete class) extends LifecycleEntity
            │
            Fields:
            - Map<String, SegmentedStorageTopic> internalTopics
            - boolean grouped
            - TopicCapacityPolicy capacity
            - String nfrFilterName
            - [NEW] Set<RegionName> replicationRegions
            - [NEW] Set<RegionName> produceRegions
            - [NEW] boolean autoFailover
            - [NEW] Set<TopicTag> tags
            - [NEW] Map<RegionName, RegionName> failoverRegion  // failed region → target region
```

### Current VaradhiTopic Structure

**Inherited Fields (from LifecycleEntity):**
- `name` (String) - Fully qualified topic name (e.g., "project.topic")
- `version` (int) - Version of the topic
- `status` (LifecycleStatus) - Current lifecycle state

**Core Fields:**
- `internalTopics` (Map<String, SegmentedStorageTopic>) - Maps region names to storage topics
- `grouped` (boolean) - Whether messages are grouped/ordered
- `capacity` (TopicCapacityPolicy) - Capacity limits (QPS, throughput, read fan-out)
- `nfrFilterName` (String) - NFR filter name (nullable)

**Current Limitations:**
- Single region or basic replication only
- No explicit region management
- No failover capabilities
- No topic classification/tags
- Region names stored as plain strings (no type safety)

### Storage Topic Abstraction

**StorageTopic** - Abstract class representing topics in underlying messaging systems (Pulsar, etc.)
- **Fields:**
  - `id` (int) - Unique identifier
  - `name` (String) - Storage topic name
- **Methods:**
  - `String getName()` - Returns the topic name

**PulsarStorageTopic** - Pulsar-specific implementation
- **Extends:** `StorageTopic`
- **Fields:**
  - `id` (int) - Inherited from StorageTopic
  - `name` (String) - Inherited from StorageTopic
  - `partitionCount` (int) - Number of partitions for the Pulsar topic

**SegmentedStorageTopic** - Wrapper for partition scaling
- **Fields:**
  - `storageTopics` (StorageTopic[]) - Array of storage topics
  - `activeStorageTopicId` (int) - ID of the currently active storage topic
  - `produceIndex` (int) - Index of the topic to use for producing
  - `topicState` (TopicState) - Current state (Producing, Blocked, Throttled, Replicating)
- **Methods:**
  - `StorageTopic getTopicToProduce()` - Returns the topic to use for producing
  - `StorageTopic getTopic(int id)` - Retrieves a topic by ID
  - `List<StorageTopic> getActiveTopics()` - Returns all active topics

---

## Proposed: Global Topics Support

### New Fields for VaradhiTopic

```java
// Multi-region support
private final Set<RegionName> replicationRegions;           // Where topic is replicated
private final Set<RegionName> produceRegions;               // Where messages can be produced

// Failover configuration
private final boolean autoFailover;                         // Enable automatic failover
private final Map<RegionName, RegionName> failoverRegion;   // failed region → target region (topic failover)

// Classification
private final Set<TopicTag> tags;                           // PROD, NON_PROD, HIGH_PRIORITY
```

**failoverRegion** supports single-topic failover: when Pulsar (or the message stack) is unavailable for this topic in one region, traffic can be routed to another replicated region. The map is mutable at runtime so the controller can set/clear entries when failover is triggered or reverted. Methods: `getFailoverRegion()`, `getFailoverTarget(RegionName)`, `addFailover(RegionName, RegionName)`, `removeFailover(RegionName)`.

### Project.replicationRegions

**Project** now has a mandatory **replicationRegions** (`Set<RegionName>`):

- Default replication regions for all topics under this project.
- When a topic is created **without** specifying `replicationRegions`, it inherits this value from the project.
- Mandatory at project level (at least one region); validated in the constructor.
- Topic creation via `TopicResource.toVaradhiTopic(Project project)` uses `project.getReplicationRegions()`.

### New Supporting Classes

**RegionName** - Value object for type-safe region names
- **Fields:**
  - `value` (String) - The region name string (final, validated)
- **Methods:**
  - `static RegionName of(String value)` - Factory method
  - `String getValue()` - Returns the string value
  - `String toString()` - Returns the string value

**Region** - Represents a region with availability status
- **Fields:**
  - `name` (RegionName) - The name of the region (final)
  - `status` (RegionStatus) - Current availability status (mutable)
- **Methods:**
  - `static Region of(RegionName name)` - Creates with AVAILABLE status
  - `static Region of(String name)` - Creates with AVAILABLE status
  - `static Region of(RegionName name, RegionStatus status)` - Creates with specified status
  - `String getName()` - Returns the region name as string
  - `boolean isProduceAvailable()` - Checks if produce is available
  - `boolean isConsumeAvailable()` - Checks if consume is available
  - `boolean isAvailable()` - Checks if region is fully available

**RegionStatus** - Enum for region availability status
- **Values:**
  - `AVAILABLE` - Region is fully available for both produce and consume
  - `UNAVAILABLE` - Region is completely unavailable
  - `PRODUCE_UNAVAILABLE` - Region available for consume but not produce
  - `CONSUME_UNAVAILABLE` - Region available for produce but not consume
- **Methods:**
  - `boolean isProduceAvailable()` - Checks if produce operations are available
  - `boolean isConsumeAvailable()` - Checks if consume operations are available
  - `boolean isAvailable()` - Checks if region is fully available

**TopicTag** - Enum for topic classification
- **Values:**
  - `PROD` - Production environment
  - `NON_PROD` - Non-production environment  
  - `HIGH_PRIORITY` - High priority topic

### MemberInfo (proposed change)

**MemberInfo** should include a **RegionName** field so that cluster members can be distinguished by region.

- **Purpose:** Enables the system to identify which region a member (node) belongs to, which is required for multi-region topology, failover decisions, and region-aware routing.
- **Proposed field:** `RegionName region` (or `regionName`) on `MemberInfo`.
- **Usage:** When registering or discovering members, the region can be used to route traffic, enforce produce/consume region constraints, and determine failover targets.

### RegionHandler (proposed)

**RegionHandler** is a component for managing regions in Varadhi. It exposes administrative APIs for the lifecycle of regions.

**Responsibility:** Manage region metadata and availability status (create, update status, delete).

**APIs:**

| API | Description |
|-----|-------------|
| **Create Region** | Register a new region (e.g. with a name and initial status). Used when onboarding a new data center or zone. |
| **Update Region Status** | Update the availability status of an existing region (e.g. set to `AVAILABLE`, `UNAVAILABLE`, `PRODUCE_UNAVAILABLE`, `CONSUME_UNAVAILABLE`). Used during incidents or recovery. |
| **Delete Region** | Remove a region from the system. Typically used when decommissioning a zone; may be guarded by checks (e.g. no topics/subscriptions still using the region). |

**Notes:**
- RegionHandler works with the **Region** and **RegionName** entities and **RegionStatus** enum.
- These APIs are intended for administrative/operational use (e.g. by controllers or ops tooling), not for regular produce/consume traffic.

### Key Changes

1. **Type Safety**: `RegionName` replaces `String` for region identifiers
2. **Multi-Region Support**: Explicit `replicationRegions` and `produceRegions` configuration
3. **Failover**: `autoFailover` flag enables automatic failover on failures
4. **Classification**: `tags` for operational management
5. **Simplification**: Removed redundant `ordered` field (uses `grouped` instead)

### Updated VaradhiTopic Methods

**Factory Methods:**
```java
// Default Global Topic
VaradhiTopic.of(project, name, grouped, capacity, actionCode, replicationRegions)

// Full configuration
VaradhiTopic.of(project, name, grouped, capacity, actionCode, nfrStrategy,
                replicationRegions, produceRegions, autoFailover, tags)
```

**Region Queries:**
```java
boolean canProduceInRegion(RegionName regionName)
boolean isReplicatedInRegion(RegionName regionName)
```

**Topic Type Queries:**
```java
boolean isGlobalTopic()              // Multiple regions
boolean isLocalTopic()               // Single region
boolean supportsFailover()           // Multi-region + auto-failover
boolean supportsMessageFailureFailover()  // Ungrouped + failover
boolean supportsTopicFailureFailover()     // Multi-region + failover
```

**Ordering:**
```java
String getOrderingSemantics()  // "Mostly Ordered" if grouped, "Unordered" otherwise
```

---

## Design Decisions

### 1. Type-Safe Region Names
- **Rationale**: Prevents errors from invalid region strings
- **Implementation**: `RegionName` value object with validation
- **Usage**: `Set<RegionName>` instead of `Set<String>`

### 2. Explicit Region Configuration
- **Rationale**: Clear separation between replication and produce regions
- **Default**: Produce regions default to all replication regions if not specified
- **Validation**: Produce regions must be subset of replication regions

### 3. Failover Configuration
- **Rationale**: Users need control over automatic failover behavior
- **Default**: `autoFailover = true` for high availability
- **Opt-out**: Users can disable to prevent ordering loss during failover

### 4. Topic Classification
- **Rationale**: Operational management and capacity planning
- **Tags**: PROD, NON_PROD, HIGH_PRIORITY
- **Extensible**: Enum can be extended with more tags

---

## Migration Path

### Backward Compatibility

1. **Legacy Factory Methods**: Deprecated but still functional
   - Create topics with empty replication regions
   - Will be populated during migration phase

2. **Internal Topics Map**: Still uses `String` keys for region names
   - Maintains compatibility with storage layer
   - Can be migrated to `RegionName` keys in future

3. **Grouped Field**: Maintained as-is
   - No changes to existing behavior
   - Used for ordering semantics

### Migration Steps

1. **Existing Topics**: Auto-migrated to Global Topics
   - Replication regions populated from project defaults
   - Legacy fields preserved

2. **New Topics**: Must specify replication regions
   - Use new factory methods with `Set<RegionName>`
   - Default to project's replication regions

3. **API Updates**: 
   - Update topic creation APIs to accept `RegionName` sets
   - Update queries to use type-safe region names

---

## Example Usage

### Creating a Global Topic

```java
// Create replication regions
Set<RegionName> replicationRegions = Set.of(
    RegionName.of("HYD"),
    RegionName.of("CH2")
);

// Create topic with default config
VaradhiTopic topic = VaradhiTopic.of(
    "project1",
    "topic1",
    false,  // not grouped (unordered)
    new TopicCapacityPolicy(100, 1000, 2),
    LifecycleStatus.ActionCode.USER_ACTION,
    replicationRegions
);
```

### Creating a Topic with Full Configuration

```java
Set<RegionName> replicationRegions = Set.of(
    RegionName.of("HYD"),
    RegionName.of("CH2")
);

Set<RegionName> produceRegions = Set.of(RegionName.of("HYD")); // Restrict to HYD

Set<TopicTag> tags = Set.of(TopicTag.PROD, TopicTag.HIGH_PRIORITY);

VaradhiTopic topic = VaradhiTopic.of(
    "project1",
    "topic1",
    true,  // grouped (ordered)
    new TopicCapacityPolicy(200, 2000, 3),
    LifecycleStatus.ActionCode.USER_ACTION,
    null,  // no NFR filter
    replicationRegions,
    produceRegions,
    true,  // auto-failover enabled
    tags
);
```

### Querying Topic Properties

```java
// Check if topic is global
if (topic.isGlobalTopic()) {
    // Handle global topic logic
}

// Check if produce is allowed in a region
RegionName region = RegionName.of("HYD");
if (topic.canProduceInRegion(region)) {
    // Produce to this region
}

// Get ordering semantics
String semantics = topic.getOrderingSemantics(); 
// Returns "Mostly Ordered" if grouped, "Unordered" otherwise
```

---

## Summary of Changes

| Aspect | Current | Proposed |
|--------|--------|----------|
| **Region Management** | String-based, implicit | Type-safe `RegionName`, explicit sets |
| **Replication** | Single region or basic | Multi-region with explicit configuration |
| **Failover** | Not supported | Configurable auto-failover |
| **Classification** | None | Topic tags (PROD, NON_PROD, etc.) |
| **Ordering** | `grouped` + `ordered` (redundant) | `grouped` only |
| **Type Safety** | String regions | `RegionName` value objects |

---

## Benefits

1. **Type Safety**: `RegionName` prevents invalid region strings
2. **Clarity**: Explicit region configuration makes behavior clear
3. **Flexibility**: Configurable produce regions and failover
4. **Operational**: Tags enable better capacity planning and management
5. **Simplicity**: Removed redundant `ordered` field
6. **Extensibility**: Easy to add more regions, tags, or failover strategies

---

## Next Steps

1. **Review**: Get feedback on proposed structure
2. **Implementation**: Update topic creation/update APIs
3. **Migration**: Plan migration of existing topics
4. **Testing**: Validate failover behavior
5. **Documentation**: Update API documentation with new fields
