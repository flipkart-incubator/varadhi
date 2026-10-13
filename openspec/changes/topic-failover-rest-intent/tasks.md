# Tasks

## 1. Bus and controller intent

- [x] 1.1 FailoverApiRequest + ControllerApi/RemoteClient create/get/abort/list
- [x] 1.2 TransitionService create persists master + op; abort/get/list without executor
- [x] 1.3 Resume hook may warn orchestrator-not-wired (acceptable until orchestrator stack)

## 2. REST

- [x] 2.1 TopicFailoverHandlers POST/GET/abort + WebServerVerticle routes
- [x] 2.2 Auth TOPIC_UPDATE / TOPIC_GET; 202 on create/abort

## 3. Explicit non-goals (other changes)

- [ ] Knobs + enriched GET + preflight/history/produce-config/Retry-After — `topic-failover-rest-api`
- [ ] Executor / barriers / lag — `internal-topic-failover`

## Workflow

- Tip of `#339` peel for REST MVP
- Parallel bases for rest-api and orchestrator branches
