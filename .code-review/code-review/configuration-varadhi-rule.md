# Rule Catalog — Configuration & Options Objects (Varadhi-specific)

## Scope
Varadhi-specific configuration conventions: the `*Options`/`*Config` Lombok pattern used for YAML-bound config POJOs, jakarta-validation of the top-level `AppConfiguration`, and forward-compatible JSON binding.

- Key classes:
  - Top-level config: `core/src/main/java/com/flipkart/varadhi/core/config/AppConfiguration.java`, `controller/.../config/ControllerConfiguration.java`
  - Options POJOs: `core/.../config/ProducerOptions.java`, `pulsar/.../config/*Options.java`, `spi/.../MetaStoreOptions.java`, `web/.../config/RestOptions.java`
  - Load/validate seam: `server/src/main/java/com/flipkart/varadhi/VaradhiApplication.java` (`readConfiguration`)

---

## Rules

### Validate top-level configuration with jakarta-validation and `Validatable`

- Category: best practices
- Severity: critical
- Description: Top-level configuration classes implement `Validatable` and annotate fields with jakarta-validation constraints — `@NotNull`/`@NotBlank` for required fields, `@Size` for bounds, and `@Valid` to cascade into nested config objects (see `AppConfiguration`). Validation runs at load time so the process fails fast at startup on bad config rather than NPE-ing deep in a request path. A new required config field added without `@NotNull`/`@Valid` lets a misconfigured node boot and fail unpredictably later.
- Suggested fix:
  - Annotate required fields with `@NotNull`/`@NotBlank`; bound strings/collections with `@Size`.
  - Mark nested config objects `@Valid` so their constraints are also enforced.
  - Implement `Validatable` (and trigger `validate()`); confirm the new field is exercised by config validation at startup.

---

### Follow the `*Options` Lombok pattern: `@Data @Builder @NoArgsConstructor @AllArgsConstructor` with `@Builder.Default` + a static default factory

- Category: best practices
- Severity: suggestion
- Description: Nested config/option POJOs use a consistent shape (`ProducerOptions` is the reference): `@Data @Builder @NoArgsConstructor @AllArgsConstructor`, per-field defaults via `@Builder.Default`, and a static `defaultOptions()`/`getDefault()` factory. The no-arg constructor is required for Jackson/YAML deserialization; `@Builder.Default` ensures the same defaults whether the object is built programmatically or partially specified in YAML; the static factory gives call sites and tests a single source of defaults. Unlike persisted entities (which are immutable `@Getter` value objects — see `entity-serialization-varadhi-rule.md`), these mutable `@Data` POJOs are config DTOs, not domain entities.
- Suggested fix:
  - Name config objects `*Options` (or `*Config`) and apply `@Data @Builder @NoArgsConstructor @AllArgsConstructor`.
  - Give defaulted fields `@Builder.Default` and expose a static `defaultOptions()` that returns `builder().build()`.
  - Do not reuse this mutable `@Data` shape for ZK-persisted entities.

---

### Tolerate unknown properties on externally-supplied config with `@JsonIgnoreProperties(ignoreUnknown = true)`

- Category: best practices
- Severity: suggestion
- Description: Top-level configuration classes are annotated `@JsonIgnoreProperties(ignoreUnknown = true)` (see `AppConfiguration`) so a YAML file carrying a newly-introduced or environment-specific key does not crash older binaries during a rolling deploy. This is the deliberate forward-compatibility convention for operator-supplied config; it should be applied to config roots, not to strict wire/entity types where an unknown field is a genuine error.
- Suggested fix:
  - Add `@JsonIgnoreProperties(ignoreUnknown = true)` to configuration roots that bind operator-supplied YAML.
  - Keep strict (no-ignore) deserialization for persisted entities and RPC payloads where unexpected fields should surface as errors.
