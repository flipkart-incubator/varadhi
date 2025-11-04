# Copilot Instructions for Varadhi

## Project Overview
- **Varadhi** is a message bus with a REST interface, supporting both point-to-point and pub-sub models. It is designed for async REST communication between microservices, using HTTP as the transport.
- modules: `entities`, `spi`, `common`, `core`, `producer`, `pulsar`, `consumer`, `controller`, `web-spi`, `web`, `server`, `metastore-zk`.
- The main entry point is `server/src/main/java/com/flipkart/varadhi/VaradhiApplication.java`.

## Architecture & Data Flow
- **Server** module is the entry point, orchestrating all other modules.
- **Entities** define the data models used across the system. For core system and web API.
- **SPI** provides interfaces for persistence and messaging; implementations are in `pulsar` (messaging) and `metastore-zk` (metadata store).
- **Core** contains business logic and is used by most modules.
- **Controller**, **Consumer**, and **Web** modules are deployed as Vert.x verticles, managed by the main application.
- **Controller** handles subscription management and allocation, periodic jobs, message production rate limiting.
- **Consumer** handles message consumption and delivery to subscriber's endpoints.
- **Web** exposes the REST API for clients to interact with Varadhi, including producing messages, managing subscriptions, handling sidelined messages, and monitoring.
- **Component roles:** Configured via YAML (`member.roles`) - each instance can run multiple roles (Server, Controller, Consumer).
- **External dependencies:**
  - [Apache Pulsar](https://pulsar.apache.org/) for message brokering
  - [Zookeeper](https://zookeeper.apache.org/) for metadata
  - [Vert.x](https://vertx.io/) for async/reactive programming

## Module Structure
Refer to `docs/module-structure.png` for a visual representation of module dependencies and interactions.

## Build & Test Workflows
- **Build:** `./gradlew build test`
- **Integration/E2E tests:**
  1. `./gradlew copyDependencies copyE2EConfig -x test`
  2. `docker compose --profile test -f setup/docker/compose.yml up -d --wait --wait-timeout 180`
  3. `./gradlew testE2E`
- **Run server locally:**
  - `./gradlew run` (uses config from `server/src/main/resources/configuration.yml` or override via `config.overrides`)
- **Local Docker setup:** Use profiles (`dev`, `test`, `service`) for different environments
- **Kubernetes/Helm:** See `setup/helm/` for deployment charts.

## Conventions & Patterns
- **Code style:**
  - 4 spaces indentation, no tabs
  - Eclipse JDT formatter via `codestyle.xml` (enforced by Spotless)
  - Run `./gradlew spotlessApply` to auto-format
- **Configuration:**
  - YAML files in `conf/` and `setup/docker/`
  - Linked configs resolved at runtime (see `VaradhiApplication.readConfiguration`)
  - Config overrides via `src/*/resources/config.overrides` files
- **Testing:**
  - Separate `testE2E` source sets for integration tests
  - Use real DB/broker for integration tests (TestingServer for ZK)
  - Avoid sleeps/timing assumptions in tests
  - Prefer minimal mocking; avoid static mocks
  - Prefer to implement in memory implementations / mock implementations for interfaces wherever possible
  - Use `@ExtendWith(VertxExtension.class)` for async tests
- **Module boundaries:**
  - Only use APIs/interfaces from other modules, not internal classes
  - All cross-component communication is via HTTP or message bus

## Key Gradle Patterns
- **Version catalog:** All dependencies managed in `gradle/libs.versions.toml`
- **Build conventions:** Shared logic in `buildSrc/src/main/groovy/com.flipkart.varadhi.java-common-conventions.gradle`
- **Test fixtures:** Modules use `java-test-fixtures` plugin for shared test utilities
- **Config copying:** Dynamic config generation via `copyConfigForModule()` and `copyConfigToDir()` functions
- **Java 21:** Project uses Java 21 with `--parameters` compiler flag for reflection
- **Legacy libraries:** Use classpath (not module-path) for non-modularized JARs like Curator

## Deployment & Configuration
- **Multi-role deployment:** Single JAR can run different component combinations via `member.roles`
- **Linked configurations:** Main config references other YAML files resolved at runtime
- **Environment variables:** Docker configs use `${VAR}` expansion for environment-specific values
- **Health checks:** REST endpoint at `/v1/health-check` returns "iam_ok"
- **Clustering:** Vert.x cluster manager with ZooKeeper for inter-node communication

## Examples & References
- Main application: `server/src/main/java/com/flipkart/varadhi/VaradhiApplication.java`
- Build config: `build.gradle`, `server/build.gradle`, `settings.gradle`
- Docker/K8s: `setup/docker/`, `setup/helm/`
- Code style: `codestyle.xml`, enforced by Spotless
- Documentation: local folder `docs/wiki/` and on github [Wiki](https://github.com/flipkart-incubator/varadhi/wiki/)

---

**For more details, see the [README.md](../README.md) and [Wiki](https://github.com/flipkart-incubator/varadhi/wiki/).**

