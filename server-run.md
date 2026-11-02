# Local Server Run & API Verification

- **Date:** 2026-11-02
- **Branch:** `feature-topicFailoverControllerSide`
- **Server Endpoint:** `http://localhost:18488`
- **Dependencies:** Docker Compose `zookeeper` and `pulsar` (Profile: `dev`)

---

## 1. Startup Log Summary

The server started successfully and bound all verticles and cluster roles (`Server`, `Controller`, `Consumer`):

```text
2026-11-02 17:49:20 INFO  WebServerVerticle:388 - HttpServer started on port 18488
2026-11-02 17:49:20 INFO  WebServerVerticle:221 - WebServer verticle started successfully
2026-11-02 17:49:20 INFO  ControllerVerticle:121 - Controller started successfully in region RegionName[value=default]
2026-11-02 17:49:20 INFO  VaradhiApplication:127 - Started successfully on BLRETV-YWCP2P6V.local
```

Full raw logs were written to `/tmp/varadhi-server-run.log`.

---

## 2. API Data & Endpoint Testing

### Authentication & Superuser behavior (`x_user_id`)

The configured superuser in `conf/authorizationConfig.yml` is **`thanos`**.

| Endpoint | Header / Identity | Status Code | Response / Result | Notes |
|---|---|---|---|---|
| `GET /v1/health-check` | Anonymous | `200` | `"iam_ok"` | Unauthenticated health check |
| `GET /v1/regions` | `x_user_id: admin` | `403` | `{"reason":"User admin is not authorized..."}` | Forbidden (non-superuser without root list permission) |
| `GET /v1/regions` | `x_user_id: thanos` | `200` | `[{"name":"default", ...}, {"name":"region2", ...}]` | Success (superuser authorized) |
| `GET /v1/orgs` | `x_user_id: thanos` | `200` | `[{"name":"public_org", ...}]` | Lists organizations |
| `GET /v1/projects` | `x_user_id: thanos` | `405` | Method Not Allowed | Route does not support `GET /v1/projects` directly |
| `GET /v1/projects/public` | `x_user_id: thanos` | `200` | `{"name":"public", "org":"public_org", ...}` | Fetches created project details |
| `GET /v1/projects/public/topics` | `x_user_id: thanos` | `200` | `["topic1"]` | Lists project topics |
| `GET /v1/projects/public/failovers` | `x_user_id: thanos` | `200` | `[]` | Lists active topic failover operations |
| `GET /v1/projects/public/topics/topic1/failover` | `x_user_id: thanos` | `404` | `{"reason":"Transition(public.topic1) not found."}` | No active failover transition in progress |

---

## 3. Entity Creation & Failover Validation Flow

Using `x_user_id: thanos`:

1. **Create Org (`public_org`):**
   - `POST /v1/orgs` → `200` (`{"name": "public_org", ...}`)

2. **Create Team (`public_team`):**
   - `POST /v1/orgs/public_org/teams` → `200` (`{"name": "public_team", ...}`)

3. **Create Project (`public`):**
   - `POST /v1/projects` → `200` (`{"name": "public", ...}`)

4. **Register Region (`region2`):**
   - `POST /v1/regions` → `200` (`{"name": "region2", ...}`)

5. **Create Topic (`topic1`):**
   - `POST /v1/projects/public/topics` → `200` (`{"name": "topic1", ...}`)

6. **Failover Validations Tested:**
   - **Same Source/Target Region:**
     `POST /v1/projects/public/topics/topic1/failover` with `{"sourceRegion": "default", "targetRegion": "default"}`
     → `400 Bad Request`: `{"reason":"sourceRegion and targetRegion must differ."}`
   - **Target Region Not Configured on Topic:**
     `POST /v1/projects/public/topics/topic1/failover` with `{"sourceRegion": "default", "targetRegion": "region2"}`
     → `400 Bad Request`: `{"reason":"Topic public.topic1 is not configured for targetRegion region2."}`
