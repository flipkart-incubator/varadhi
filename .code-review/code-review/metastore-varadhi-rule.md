# Rule Catalog — MetaStore & Entity Design (Varadhi-specific)

## Scope
Varadhi-specific entity design rules: the `Versioned → MetaStoreEntity → LifecycleEntity` hierarchy, `@ValidateResource` / `Validatable`, FQN-based tenant scoping, ZooKeeper znode path design, and `JsonMapper` usage.

- Key directories:
  - Entities: `entities/src/main/java/com/flipkart/varadhi/entities/`
  - Store interfaces: `spi/src/main/java/com/flipkart/varadhi/spi/db/`
  - ZK store implementation: `metastore-zk/src/main/java/com/flipkart/varadhi/db/`

---

## Rules

### New persisted entities must extend the correct level of the `Versioned` hierarchy

- Category: best practices
- Severity: critical
- Description: Varadhi uses a three-level entity inheritance pattern for all ZooKeeper-persisted entities. `Versioned` (name + version) is the required base for optimistic concurrency. `MetaStoreEntity` adds `MetaStoreEntityType` for store dispatch. `LifecycleEntity` adds `LifecycleStatus` for state-machine entities. New entities that bypass this hierarchy lose version tracking, breaking ZK `setData` optimistic locking and making concurrent updates silently destructive.
  - Use `MetaStoreEntity` for simple, version-tracked entities (`Org`, `Project`, `Region`).
  - Use `LifecycleEntity` for entities with explicit lifecycle states (`VaradhiTopic`, `VaradhiSubscription`).
  - Never omit `version` from the `@JsonCreator` constructor — it must be deserialized to restore the current ZK version.
- Suggested fix: When adding a new persisted entity, determine which hierarchy level it belongs to and extend accordingly. Use a static factory method (`Entity.of(...)`) to enforce construction-time invariants and set the initial version.

---

### Annotate all new `MetaStoreEntity` subclasses with `@ValidateResource`

- Category: security
- Severity: critical
- Description: `@ValidateResource` triggers `ResourceValidator` which enforces name constraints (allowed characters, length limits, reserved prefixes). Skipping it allows names containing `/`, `.`, or other characters that have structural meaning in ZooKeeper znode paths and in Varadhi's FQN scheme. A name like `"my/org"` would silently create a two-level znode subtree instead of a single node, corrupting the metastore.
- Suggested fix:
  - Annotate every `MetaStoreEntity` subclass with `@ValidateResource(message = "Invalid <Entity> name. Check naming constraints.")`.
  - Implement `Validatable` and ensure validation is triggered at the handler layer via `ctx.body().asValidatedPojo(Entity.class)`, which calls `validate()` automatically.
---

### Scope project-owned entities by FQN (`project.resourceName`), not short name

- Category: security
- Severity: critical
- Description: Varadhi's multi-tenancy is enforced by using fully-qualified names (FQNs) as ZK store keys. An entity that belongs to a project must use the FQN `project.resourceName` as its primary identifier; using only the short `resourceName` allows different projects' entities to collide at the same ZK path, so tenant A can read or modify tenant B's resources. The FQN is available as `entity.getName()` after construction with `VaradhiTopic.of(topicName, projectName, ...)`. System-global entities (e.g., `Region`, `Org`) are explicitly exempt and documented as such.
- Suggested fix:
  - Use `entity.getName()` (the FQN) as the ZK store key in all `*Store` implementations.
  - Propagate the FQN through all service method signatures so callers cannot accidentally pass a short name.
  - Document any globally-scoped entity with a comment explaining why it is not project-scoped.
---

### Prefer flat `/entity-type/fqn` znode paths; avoid deep hierarchies unless listing is required

- Category: performance
- Severity: suggestion
- Description: ZooKeeper znode depth affects watch management, `getChildren` latency, and path-construction complexity. A flat structure `/varadhi/topics/project.topicName` is simpler to reason about and cheaper to operate on than a deeply nested `/varadhi/orgs/org1/projects/proj1/topics/topic1`. Prefer flat paths where subtree listing is not needed; use a hierarchy only when you explicitly need `getChildren` on an intermediate node.
- Suggested fix:
  - Use flat `/entity-type/fqn` paths by default.
  - Add hierarchy only when the feature requires listing all children of a parent (e.g., "list all subscriptions of a topic").
  - Before adding a new path structure, verify there is no existing overlapping path in `VaradhiMetaStore` that achieves the same purpose.
---

### Use `JsonMapper` for all ZK serialization/deserialization; never instantiate `ObjectMapper` directly

- Category: best practices
- Severity: suggestion
- Description: Varadhi centralizes its Jackson `ObjectMapper` in `JsonMapper` (registered with `ParameterNamesModule`, `Jdk8Module`, and Lombok compatibility settings, compiled with the `-parameters` flag). ZK store implementations that create their own `ObjectMapper` will silently fail to deserialize `@Value` classes because the `-parameters` metadata is only matched when `ParameterNamesModule` is present. All serialization in `metastore-zk/` must go through `JsonMapper`.
- Suggested fix:
  - Use `JsonMapper.toJson(entity)` for serialization and `JsonMapper.fromJson(bytes, EntityClass.class)` for deserialization.
  - Never call `new ObjectMapper()` in any store or persistence code.
  - For custom serializers/deserializers, register them on the centralized `JsonMapper` instance rather than on a local mapper.
