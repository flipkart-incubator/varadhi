# Rule Catalog — Error Handling & Exception Model (Varadhi-specific)

## Scope
Varadhi-specific error-handling rules: the `VaradhiException` hierarchy in `common.exceptions`, the single web-layer `FailureHandler` that maps domain exceptions to HTTP status codes, and how error messages are surfaced to callers.

- Key classes:
  - Exception base: `common/src/main/java/com/flipkart/varadhi/common/exceptions/VaradhiException.java`
  - Domain exceptions: `common/src/main/java/com/flipkart/varadhi/common/exceptions/` (`ResourceNotFoundException`, `DuplicateResourceException`, `InvalidOperationForResourceException`, `ServerNotAvailableException`, `ProduceException`, …)
  - HTTP translation: `web/src/main/java/com/flipkart/varadhi/web/FailureHandler.java`

---

## Rules

### All domain exceptions must extend `VaradhiException` and use `@StandardException`

- Category: best practices
- Severity: critical
- Description: Varadhi's exception hierarchy is rooted at `VaradhiException extends RuntimeException`, declared with Lombok's `@StandardException` (which generates the no-arg, message, cause, and message+cause constructors). Every domain/service/store error must be expressed as a `VaradhiException` subclass in `common.exceptions`. Throwing a raw `RuntimeException`, `IllegalStateException`, or a transport-specific exception from `core`/`spi`/store code defeats the centralized HTTP translation in `FailureHandler` (it falls through to HTTP 500) and, for non-`VaradhiException` types, causes the internal cause message to be appended to the client-facing error (see the message-exposure rule below).
- Suggested fix:
  - Add a new exception in `common/.../exceptions/` that `extends VaradhiException` and is annotated `@StandardException`.
  - Throw the typed domain exception from the service/store layer; never throw bare `RuntimeException` or HTTP exceptions from `core`/`spi`.

---

### Register every new domain exception in `FailureHandler.getStatusCodeFromFailure`

- Category: best practices
- Severity: critical
- Description: `FailureHandler` is the single place where a thrown exception is mapped to an HTTP status code, via an explicit `instanceof`/`Class ==` ladder in `getStatusCodeFromFailure(Throwable)` (e.g. `DuplicateResourceException → 409`, `ResourceNotFoundException → 404`, `InvalidOperationForResourceException → 409`, `ServerNotAvailableException → 503`). Any exception type not present in that ladder defaults to `HTTP_INTERNAL_ERROR` (500). Introducing a new domain exception without adding its mapping silently turns an expected 4xx into a 500, breaking client contracts and alerting.
- Suggested fix:
  - When adding a new `VaradhiException` subclass that should produce a non-500 response, add a corresponding branch in `FailureHandler.getStatusCodeFromFailure`.
  - Note the mapping uses exact-class comparison (`tClazz == X.class`) in places — verify subclasses are matched the way you intend.

---

### Let `FailureHandler` set status codes; never set HTTP status from services or handlers

- Category: maintainability
- Severity: critical
- Description: Handlers must terminate failed flows with `ctx.fail(throwable)` and let the global `FailureHandler` derive the status code and serialize the `ErrorResponse`. Service/`core` code must remain transport-agnostic and throw `VaradhiException` subclasses only. Setting `response.setStatusCode(...)` directly in a handler, or importing `HttpResponseStatus`/`HttpException` into `core`, scatters HTTP policy across the codebase and bypasses the consistent JSON error envelope.
- Suggested fix:
  - In handlers, end async chains with `.onFailure(ctx::fail)` (see the Vert.x rule catalog) and do not hand-craft error responses.
  - Keep `io.netty`/`io.vertx` HTTP types out of `core`/`spi`; throw domain exceptions and map them centrally.

---

### Do not expose internal cause details on client-facing errors

- Category: security
- Severity: suggestion
- Description: `FailureHandler.getErrorFromFailure` appends the second-level cause message (`"Internal error : " + cause.getMessage()`) only when the outermost throwable is **not** a `VaradhiException`. This means leaking raw infrastructure messages (ZooKeeper/Pulsar stack details) to API clients is a direct consequence of throwing a non-`VaradhiException` to the web layer. Wrapping internal failures in a typed `VaradhiException` with a curated message keeps internal details out of the response body (they are still logged server-side at `ERROR` for 500s).
- Suggested fix:
  - Catch low-level infrastructure exceptions at the boundary and rethrow as a `VaradhiException` subclass with a safe, caller-appropriate message.
  - Rely on `FailureHandler`'s server-side `log.error(failureLog, ctx.failure())` (only emitted for 500s) for the full diagnostic detail.
