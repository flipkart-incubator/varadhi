# Agent guide (Cursor, Claude Code)

Repo map and workflows for AI assistants. **Design and architecture:** read [`docs/README.md`](docs/README.md) (L1 system-context → L2 containers → L3 components → flows) before changing shared seams. **Implementation style:** [`.cursor/rules/implementation-guidelines.mdc`](.cursor/rules/implementation-guidelines.mdc). **Human contributors:** [`CONTRIBUTING.md`](CONTRIBUTING.md).

## What this is

Varadhi is a message bus with a REST interface (point-to-point and pub-sub). Async REST communication between microservices over HTTP. Entry point: `server/src/main/java/com/flipkart/varadhi/VaradhiApplication.java`.

**External runtime:** Apache Pulsar (messaging), ZooKeeper (metadata), Vert.x (async runtime and clustering).

## Modules

| Module | Role |
|---|---|
| `entities` | Shared data models for APIs and persistence |
| `spi` | Persistence and messaging interfaces |
| `common` | Shared utilities |
| `core` | Core business logic |
| `producer` | Produce path (including per-topic rate limiting) |
| `pulsar` | Messaging SPI → Pulsar |
| `metastore-zk` | Metadata SPI → ZooKeeper |
| `consumer` | Message consumption and delivery |
| `controller` | Subscription lifecycle, shard assignment, cluster operations |
| `web` / `web-spi` | REST API verticle and route SPI |
| `server` | Composition root: wires modules, starts verticles |

Module dependency diagram: `docs/module-structure.png`.

**Deployment roles:** `member.roles` in YAML — a process can run Server, Controller, and/or Consumer (`ComponentKind`).

## Build and test

```bash
./gradlew build test          # unit tests + spotless/checks
./gradlew spotlessApply       # format (Eclipse JDT via codestyle.xml)
./gradlew run                 # server on host (see README Dev Setup)
```

**E2E:** full Docker workflow in [README — Integration Tests (E2E)](README.md#integration-tests-e2e). Summary: `build copyDependencies copyE2EConfig -x test` → `docker build` → `docker compose ... down -v` → `up` → `./gradlew test testE2E`. Always `down -v` before a fresh run.

**Local dependencies (dev):** `PULSAR_ADVERTISED_ADDRESS=localhost docker compose --profile dev -f setup/docker/compose.yml up -d --wait`

**Kubernetes:** `setup/helm/`

## Gradle and config

- **Version catalog:** `gradle/libs.versions.toml`
- **Conventions:** `buildSrc/src/main/groovy/com.flipkart.varadhi.java-common-conventions.gradle`
- **Java 25**, `--parameters` for reflection; classpath (not module-path) for legacy JARs (e.g. Curator)
- **Test fixtures:** `java-test-fixtures` for in-memory / fake implementations of outward-facing interfaces (DB, bus, HTTP)
- **Config:** YAML under `conf/` and `setup/docker/`; linked files resolved at runtime (`VaradhiApplication.readConfiguration`); overrides in `src/*/resources/config.overrides`
- **E2E source set:** `src/testE2E/java` (Gradle task `testE2E`)

## Testing conventions

- No sleeps or timing assumptions; use controllable clocks (e.g. `MockTicker` in `common` test fixtures) where time matters
- Prefer fakes and test fixtures over Mockito; do not extend legacy Mockito tests with new mocks
- Vert.x tests: `@ExtendWith(VertxExtension.class)`
- Respect module boundaries: use other modules' public APIs, not internal types

## Key paths

| Area | Path |
|---|---|
| Main app | `server/src/main/java/com/flipkart/varadhi/VaradhiApplication.java` |
| REST / produce wiring | `web/src/main/java/com/flipkart/varadhi/web/WebServerVerticle.java` |
| Codescope docs | `docs/` |
| Docker | `setup/docker/` |
| CI E2E workflow | `.github/workflows/e2e.yml` |

## Docs workflow

Do not edit `docs/` during feature implementation; sync separately (codescope `update-docs` skill). Honor judgments in [`docs/.codescope.memory`](docs/.codescope.memory).
<!-- codescope:start -->
## Architecture docs (codescope)

This project keeps [C4](https://c4model.com/)-inspired architecture docs under **`docs/`** (relative to this repo).

| Doc | Level | Role |
|---|---|---|
| `system-context.md` | L1 | Actors, boundaries, externals |
| `containers.md` | L2 | Deployable units and how they connect |
| `**/components.md` | L3 | Internals per container |
| `flows.md` | — | Cross-container behavior |

**When the codescope MCP server is wired** (`.cursor/mcp.json` for Cursor, `.mcp.json` for Claude), prefer:

- **`codescope_explore`** — architecture context for keywords, a node id, or a file path; omit `query` to list documented service / architecture vocabulary.
- **`codescope_show`** — verbatim sections when you already know the node id.

**CLI fallback** (same data): `codescope explore` and `codescope show`.

Reach for these before grepping `docs/` or inventing architecture names.
<!-- codescope:end -->
