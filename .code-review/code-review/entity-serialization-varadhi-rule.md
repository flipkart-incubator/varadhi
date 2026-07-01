# Rule Catalog — Entity Lombok & Polymorphic Serialization (Varadhi-specific)

## Scope
Varadhi-specific entity construction and Jackson serialization conventions: Lombok annotations for entities in the `Versioned` hierarchy, polymorphic `@JsonTypeInfo`/`@JsonSubTypes` registration, static factory construction, defensive collection copies, and `@JsonIgnore` on derived getters.

This complements `metastore-varadhi-rule.md` (which covers the hierarchy levels, `@ValidateResource`, FQN scoping, znode paths, and `JsonMapper`). For generic entity-design principles (`@Value` vs `@Data`, defensive copies), see the generic `java-code-review` skill checklist. These Varadhi rules take precedence on any conflict.

- Key directories:
  - Entities: `entities/src/main/java/com/flipkart/varadhi/entities/`
  - Representative examples: `VaradhiTopic.java`, `Project.java`, `Resource.java`, `StorageTopic.java`

---

## Rules

### Use `@Getter` + `@EqualsAndHashCode(callSuper = true)` for hierarchy entities; never `@Data` or class-level `@Setter`

- Category: best practices
- Severity: critical
- Description: Every persisted entity extends the `Versioned → MetaStoreEntity → LifecycleEntity` hierarchy, so `@Data` (and its generated `equals`/`hashCode`) is wrong — it ignores superclass fields like `name`/`version` and breaks optimistic-concurrency equality. The established convention (`Project`, `VaradhiTopic`, `StorageTopic`) is `@Getter` plus `@EqualsAndHashCode(callSuper = true)`, keeping fields `private final` wherever possible. Mutation, when genuinely required, is exposed as a narrow field-level `@Setter` on the specific field (e.g. `Project.team`, `Project.description`) — never a class-level `@Setter` that opens every field.
- Suggested fix:
  - Annotate entities with `@Getter` and `@EqualsAndHashCode(callSuper = true)`; make fields `final`.
  - If a single field must be mutable, put `@Setter` on that field only, not on the class.
  - Do not use `@Data` on any class in the `Versioned` hierarchy.

---

### Construct entities via a private constructor and a static `of(...)` factory; set `INITIAL_VERSION`

- Category: best practices
- Severity: suggestion
- Description: Entities use a private constructor plus one or more static `of(...)` factories (`Project.of`, `VaradhiTopic.of`) that enforce construction-time invariants and seed the version with `INITIAL_VERSION`. Deserialization still goes through the `@JsonCreator` constructor (which restores the persisted `version`). This keeps "new entity" creation in one place, prevents callers from passing an arbitrary starting version, and centralizes FQN assembly (`fqn(project, name)`).
- Suggested fix:
  - Add a static `of(...)` factory for new-instance creation and keep the all-args constructor private (or `@JsonCreator`-only).
  - Use `INITIAL_VERSION` for newly created entities; never hardcode `0`/`1`.
  - Validate required fields in the factory/constructor with `Objects.requireNonNull(field, "…")` (as in `VaradhiTopic`).

---

### Defensive-copy mutable collection fields at construction

- Category: best practices
- Severity: suggestion
- Description: Entities that hold mutable collections copy them on the way in rather than storing the caller's reference — e.g. `VaradhiTopic` stores `new HashMap<>(perRegionQuotaWeights)` and defaults a `null` map to an empty `HashMap`. Storing the caller's reference directly lets external code mutate entity state after construction, bypassing version tracking and breaking equality/serialization assumptions.
- Suggested fix:
  - Wrap incoming collections in a fresh copy (`new HashMap<>(src)` / `List.copyOf(src)`) in the constructor.
  - Default `null` collections to empty rather than propagating `null`.

---

### Annotate derived/computed getters with `@JsonIgnore`

- Category: best practices
- Severity: suggestion
- Description: Getters that compute a value from other fields rather than exposing a stored field must be `@JsonIgnore`d so Jackson does not serialize them as phantom properties (which then fail to round-trip on deserialization). `VaradhiTopic.getProjectName()`/`getTopicName()` (parsed from the FQN) follow this. A non-ignored derived getter silently adds a field to the persisted JSON and can collide with a real property on read-back.
- Suggested fix:
  - Add `@JsonIgnore` to any getter that derives its value (parsing, lookup, aggregation) instead of returning a backing field.
  - Verify new entities round-trip through `JsonMapper` (serialize → deserialize → equals) in a unit test.
