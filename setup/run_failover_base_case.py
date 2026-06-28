#!/usr/bin/env python3
"""Run grouped-topic failover base case; capture full API payloads and emit a timeline report."""

from __future__ import annotations

import json
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

try:
    from kazoo.client import KazooClient
except ImportError:
    print("pip install kazoo", file=sys.stderr)
    sys.exit(1)

BASE = "http://localhost:18488"
PROJECT = "default_project"
TOPIC = "failover_grouped"
HDR = {"x_user_id": "thanos", "Content-Type": "application/json"}
STAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
SETUP_DIR = Path(__file__).parent
RAW_LOG = SETUP_DIR / f"failover_validation_report_{STAMP}.log"
TIMELINE_MD = SETUP_DIR / f"failover_base_case_timeline_{STAMP}.md"

_lock = threading.Lock()
events: list[dict] = []
zk_snapshots: list[dict] = []
main_op_id: str | None = None
scenario_started = False
produce_counter = 0

ZK_TOPIC_PATH = f"/varadhi/entities/Topic/{PROJECT}.{TOPIC}"
ZK_TOPIC_FIELDS = ("topicState", "version", "grouped", "autoFailover", "regionConfigs")


def producing_region(zt: dict) -> str | None:
    configs = zt.get("regionConfigs") or {}
    allowed = [r for r, c in configs.items() if c.get("produceAllowed")]
    if len(allowed) == 1:
        return allowed[0]
    return None


def ts() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def ts_short() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%H:%M:%S.%f")[:-3]


def log(line: str = "") -> None:
    msg = f"[{ts()}] {line}" if line else ""
    with _lock:
        print(msg)
        with RAW_LOG.open("a", encoding="utf-8") as f:
            f.write(msg + "\n")


def payload_str(payload) -> str:
    if isinstance(payload, (dict, list)):
        return json.dumps(payload, separators=(",", ":"))
    return str(payload)


def record(method: str, path: str, status: int, elapsed_ms: int, payload, note: str = "") -> None:
    entry = {
        "time": ts(),
        "time_short": ts_short(),
        "method": method,
        "path": path,
        "status": status,
        "elapsed_ms": elapsed_ms,
        "payload": payload,
        "note": note,
    }
    with _lock:
        events.append(entry)
    log(f"HTTP {method} {path} -> {status} ({elapsed_ms}ms) {payload_str(payload)[:800]}")


def http(method: str, path: str, body=None, headers=None, timeout=60, note: str = ""):
    h = dict(HDR)
    if headers:
        h.update(headers)
    data = None
    if body is not None:
        if isinstance(body, (dict, list)):
            data = json.dumps(body).encode()
        elif isinstance(body, bytes):
            data = body
            h.setdefault("Content-Type", "application/octet-stream")
        else:
            data = str(body).encode()
    req = urllib.request.Request(BASE + path, data=data, headers=h, method=method)
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode()
            elapsed_ms = int((time.time() - t0) * 1000)
            payload = json.loads(raw) if raw and raw[0] in "{[" else raw
            record(method, path, resp.status, elapsed_ms, payload, note)
            return resp.status, payload
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        elapsed_ms = int((time.time() - t0) * 1000)
        try:
            payload = json.loads(raw) if raw else raw
        except Exception:
            payload = raw
        record(method, path, e.code, elapsed_ms, payload, note)
        return e.code, payload


def zk_get_topic(name: str) -> dict:
    zk = KazooClient(hosts="127.0.0.1:2181")
    zk.start()
    try:
        data, stat = zk.get(f"/varadhi/entities/Topic/{PROJECT}.{name}")
        d = json.loads(data)
        d["_zkVersion"] = stat.version
        return d
    finally:
        zk.stop()


def topic_view(topic: dict) -> dict:
    view = {k: topic.get(k) for k in ZK_TOPIC_FIELDS if k in topic}
    view["_zkVersion"] = topic.get("_zkVersion")
    return view


def snapshot_topic(phase: str, topic: dict | None = None) -> dict:
    topic = topic if topic is not None else zk_get_topic(TOPIC)
    entry = {
        "time": ts(),
        "time_short": ts_short(),
        "phase": phase,
        "topic": topic_view(topic),
    }
    with _lock:
        if zk_snapshots and zk_snapshots[-1]["topic"] == entry["topic"]:
            return zk_snapshots[-1]
        zk_snapshots.append(entry)
    log(f"ZK topic [{phase}] {payload_str(entry['topic'])}")
    return entry


def clear_transition(name: str) -> None:
    zk = KazooClient(hosts="127.0.0.1:2181")
    zk.start()
    try:
        path = f"/varadhi/entities/Transition/{PROJECT}.{name}"
        if zk.exists(path):
            zk.delete(path, recursive=True)
            log(f"ZK deleted transition {path}")
    finally:
        zk.stop()


class TopicZkWatcher:
    """Poll ZK topic during failover; record when version/state/region changes."""

    def __init__(self, interval: float = 0.15):
        self.interval = interval
        self.stop = False
        self._thread = None
        self._last_key: tuple | None = None

    def _latest_failover_stage(self) -> str:
        for e in reversed(events):
            if e["path"].endswith("/failover") and e["method"] == "GET" and e["status"] == 200:
                stage = extract_stage(e["payload"])
                if stage:
                    return stage
        return "IN_PROGRESS"

    def start(self):
        def run():
            while not self.stop:
                try:
                    topic = zk_get_topic(TOPIC)
                    view = topic_view(topic)
                    key = (view.get("version"), view.get("topicState"), producing_region(view))
                    if key != self._last_key:
                        self._last_key = key
                        phase = self._latest_failover_stage()
                        if view.get("topicState") == "Fenced":
                            phase = "SWITCH (Fenced)"
                        snapshot_topic(phase, topic)
                except Exception as ex:
                    log(f"ZK topic poll error: {ex}")
                time.sleep(self.interval)

        self._thread = threading.Thread(target=run, daemon=True, name="zk-topic-watcher")
        self._thread.start()

    def halt(self):
        self.stop = True
        if self._thread:
            self._thread.join(timeout=5)


def wait_failover_done(timeout: float = 120.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        code, body = http("GET", f"/v1/projects/{PROJECT}/topics/{TOPIC}/failover", timeout=15, note="wait_done")
        if code in (404, 500):
            reason = body.get("reason", "") if isinstance(body, dict) else str(body)
            if code == 404 or "ResourceNotFoundException" in reason or "No active failover" in reason:
                return True, (code, body)
        time.sleep(0.4)
    return False, (0, "timeout")


def ensure_producing_region(expected: str = "region-a") -> bool:
    clear_transition(TOPIC)
    zt = zk_get_topic(TOPIC)
    cur = producing_region(zt)
    log(f"ensure_producing_region current={cur} expected={expected} topicState={zt.get('topicState')}")
    if cur == expected:
        return True
    if cur is None:
        log("ensure_producing_region failed: no producing region in regionConfigs")
        return False
    code, body = http(
        "POST",
        f"/v1/projects/{PROJECT}/topics/{TOPIC}/failover",
        {"sourceRegion": cur, "targetRegion": expected, "waitForReplicationLagToClear": False},
        note=f"reset {cur}->{expected}",
    )
    if code != 200:
        log(f"ensure_producing_region failed create {code} {body}")
        return False
    done, _ = wait_failover_done(timeout=120)
    zt = zk_get_topic(TOPIC)
    ok = done and producing_region(zt) == expected
    log(
        f"ensure_producing_region result ok={ok} producing={producing_region(zt)} "
        f"state={zt.get('topicState')}"
    )
    return ok


def produce_once(mid: str):
    global produce_counter
    produce_counter += 1
    n = produce_counter
    return http(
        "POST",
        f"/v1/projects/{PROJECT}/topics/{TOPIC}/produce",
        body=b'{"load":true}',
        headers={
            "x_user_id": "thanos",
            "Content-Type": "application/octet-stream",
            "X_MESSAGE_ID": mid,
            "X_GROUP_ID": "g1",
        },
        timeout=15,
        note=f"produce #{n}",
    )


class ProduceLoad:
    def __init__(self, interval: float = 0.08):
        self.interval = interval
        self.stop = False
        self.counts: dict[int, int] = {}
        self.first_422: str | None = None
        self.last_422: str | None = None
        self.first_200_after_422: str | None = None
        self._thread = None

    def start(self):
        def run():
            i = 0
            saw_422 = False
            while not self.stop:
                i += 1
                code, _ = produce_once(f"load-{TOPIC}-{i}-{int(time.time() * 1000)}")
                self.counts[code] = self.counts.get(code, 0) + 1
                if code == 422:
                    if self.first_422 is None:
                        self.first_422 = ts_short()
                    self.last_422 = ts_short()
                    saw_422 = True
                elif code == 200 and saw_422 and self.first_200_after_422 is None:
                    self.first_200_after_422 = ts_short()
                time.sleep(self.interval)

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()

    def halt(self):
        self.stop = True
        if self._thread:
            self._thread.join(timeout=8)


class FailoverPoller:
    def __init__(self, interval: float = 0.5):
        self.interval = interval
        self.stop = False
        self._thread = None

    def start(self):
        def run():
            while not self.stop:
                http("GET", f"/v1/projects/{PROJECT}/topics/{TOPIC}/failover", timeout=15, note="poll")
                time.sleep(self.interval)

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()

    def halt(self):
        self.stop = True
        if self._thread:
            self._thread.join(timeout=5)


def fmt_payload(payload, max_len: int = 140) -> str:
    s = payload_str(payload)
    return s if len(s) <= max_len else s[: max_len - 3] + "..."


def extract_stage(payload) -> str:
    return payload.get("currentStage", "") if isinstance(payload, dict) else ""


def is_main_event(e: dict) -> bool:
    global scenario_started, main_op_id
    path = e["path"]
    if not scenario_started:
        if path.endswith("/failover") and e["method"] == "POST" and e["note"] == "trigger failover":
            scenario_started = True
            if isinstance(e["payload"], dict):
                main_op_id = e["payload"].get("operationId")
            return True
        return path.endswith("/produce") and e["note"] in ("", "produce #0")  # pre-produce before trigger
    if path.endswith("/failover") and e["method"] == "GET":
        if isinstance(e["payload"], dict) and main_op_id and e["payload"].get("operationId") not in (None, main_op_id):
            return False
        return True
    if path.endswith("/failover") and e["method"] == "POST":
        return e["note"] == "trigger failover"
    if path.endswith("/produce"):
        return True
    return False


def write_timeline(load: ProduceLoad, zt: dict, forward_ok: bool) -> None:
    produce_path = f"/v1/projects/{PROJECT}/topics/{TOPIC}/produce"
    failover_path = f"/v1/projects/{PROJECT}/topics/{TOPIC}/failover"
    main_events = [e for e in events if is_main_event(e) or (e["path"] == "/v1/health-check")]

    rows: list[tuple[str, str, str, int, str, str]] = []
    produce_422_run = 0
    last_stage = ""
    seen_stages: set[str] = set()

    if main_events and main_events[0]["path"] == "/v1/health-check":
        e = main_events[0]
        rows.append((e["time_short"], "Health check", "GET /v1/health-check", e["status"], fmt_payload(e["payload"]), ""))

    for e in main_events:
        if e["path"] == "/v1/health-check":
            continue
        t = e["time_short"]
        status = e["status"]
        payload = e["payload"]
        note = e["note"]

        if e["path"] == produce_path:
            if status == 422:
                produce_422_run += 1
                if produce_422_run == 1:
                    rows.append((t, note, "POST /produce", status, fmt_payload(payload), "First Fenced (422)"))
                continue
            if produce_422_run > 0:
                rows.append(("", f"Produce ×{produce_422_run}", "POST /produce", 422, fmt_payload({"reason": "Topic/Queue is fenced during failover..."}), f"{produce_422_run} fenced responses"))
                produce_422_run = 0
            rows.append((t, note, "POST /produce", status, fmt_payload(payload), ""))
            continue

        if e["path"] == failover_path and e["method"] == "POST":
            rows.append((t, "POST failover", "POST /failover", status, fmt_payload(payload), "region-a → region-b, waitLag=false"))
            continue

        if e["path"] == failover_path and e["method"] == "GET":
            stage = extract_stage(payload)
            if stage and stage not in seen_stages:
                seen_stages.add(stage)
                extra = ""
                if isinstance(payload, dict):
                    extra = f"topicVersionToAwait={payload.get('topicVersionToAwait')}"
                rows.append((t, f"GET failover → {stage}", "GET /failover", status, fmt_payload(payload, 180), extra))
                last_stage = stage
            elif status in (404, 500) and last_stage:
                rows.append((t, "GET failover (idle)", "GET /failover", status, fmt_payload(payload), "Transition deleted"))
            continue

    if produce_422_run > 0:
        rows.append(("", f"Produce ×{produce_422_run}", "POST /produce", 422, fmt_payload({"reason": "Topic/Queue is fenced during failover..."}), f"{produce_422_run} fenced responses"))

    counts_200 = load.counts.get(200, 0)
    counts_422 = load.counts.get(422, 0)
    fenced_window = ""
    if load.first_422 and load.last_422:
        fenced_window = f"{load.first_422} → {load.last_422}"

    lines = [
        "# Topic Failover — Base Case Timeline",
        "",
        f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"**Topic:** `{PROJECT}.{TOPIC}` (grouped, `X_GROUP_ID=g1`)",
        f"**Direction:** region-a → region-b",
        f"**Raw log:** `{RAW_LOG.name}`",
        "",
        "## Outcome",
        "",
        "| Check | Result |",
        "|-------|--------|",
        f"| Functional pass | **{'Pass' if forward_ok else 'Fail'}** |",
        f"| ZK producing region | `{producing_region(zt)}` |",
        f"| ZK topicState | `{zt.get('topicState')}` |",
        f"| Produce HTTP 200 | {counts_200} |",
        f"| Produce HTTP 422 (Fenced) | {counts_422} |",
        f"| Fenced window | {fenced_window or '—'} |",
        f"| First 200 after fence | {load.first_200_after_422 or '—'} |",
        "",
        "## Timeline (main scenario)",
        "",
        "| Time | Event | API | HTTP | Response / payload | Notes |",
        "|------|-------|-----|------|-------------------|-------|",
    ]

    for row in rows:
        t, event, api, status, resp, notes = row
        lines.append(f"| {t} | {event} | {api} | **{status}** | `{resp}` | {notes} |")

    for label, stage in (("PREPARE", "PREPARE"), ("SWITCH", "SWITCH")):
        for e in main_events:
            if e["path"] == failover_path and e["method"] == "GET" and e["status"] == 200:
                if extract_stage(e["payload"]) == stage:
                    lines.extend([
                        "",
                        f"### {label} GET response ({e['time_short']})",
                        "",
                        "```json",
                        json.dumps(e["payload"], indent=2) if isinstance(e["payload"], dict) else str(e["payload"]),
                        "```",
                    ])
                    break

    for e in main_events:
        if e["path"] == produce_path and e["status"] == 422:
            lines.extend([
                "",
                f"### First Fenced produce 422 ({e['time_short']})",
                "",
                "```json",
                json.dumps(e["payload"], indent=2) if isinstance(e["payload"], dict) else str(e["payload"]),
                "```",
            ])
            break

    lines.extend([
        "",
        "## ZK topic snapshots during failover",
        "",
        "| Time | Phase | version | topicState | producing | Snapshot |",
        "|------|-------|---------|------------|-----------|----------|",
    ])

    for s in zk_snapshots:
        t = s["topic"]
        lines.append(
            f"| {s['time_short']} | {s['phase']} | {t.get('version')} | `{t.get('topicState')}` | "
            f"`{producing_region(t)}` | see below |"
        )

    for s in zk_snapshots:
        lines.extend([
            "",
            f"### ZK topic @ {s['phase']} ({s['time_short']})",
            "",
            "```json",
            json.dumps(s["topic"], indent=2),
            "```",
        ])

    lines.extend([
        "",
        "## ZK topic after failover",
        "",
        "```json",
        json.dumps(topic_view(zt), indent=2),
        "```",
        "",
        "## Produce HTTP summary",
        "",
        f"| HTTP | Count | Phase |",
        f"|------|-------|-------|",
        f"| 200 | {counts_200} | PREPARE + post-COMPLETED |",
        f"| 422 | {counts_422} | SWITCH fenced window |",
        "",
    ])

    TIMELINE_MD.write_text("\n".join(lines), encoding="utf-8")
    log(f"Timeline report: {TIMELINE_MD}")


def wait_for_cluster_nodes(timeout_s: float = 60.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        zk = KazooClient(hosts="127.0.0.1:2181")
        zk.start()
        try:
            n = len(zk.get_children("/cluster/nodes"))
        finally:
            zk.stop()
        if n > 0:
            log(f"cluster nodes registered: {n}")
            return True
        time.sleep(2)
    log("WARN: no cluster nodes in /cluster/nodes — SWITCH barrier may complete instantly")
    return False


def main() -> int:
    global scenario_started, main_op_id, produce_counter, zk_snapshots
    RAW_LOG.write_text(f"Failover base case started {ts()}\n", encoding="utf-8")
    log(f"Report file: {RAW_LOG}")

    for _ in range(30):
        code, _ = http("GET", "/v1/health-check", timeout=5, note="health")
        if code == 200:
            break
        time.sleep(2)
    else:
        log("ERROR: server not ready")
        return 1

    wait_for_cluster_nodes()

    if not ensure_producing_region("region-a"):
        log("ERROR: could not reset to region-a")
        return 1

    clear_transition(TOPIC)
    scenario_started = False
    main_op_id = None
    produce_counter = 0
    zk_snapshots = []

    snapshot_topic("before failover")

    code, _ = produce_once(f"pre-{TOPIC}")
    if code != 200:
        log(f"ERROR pre-produce failed {code}")
        return 1

    load = ProduceLoad()
    poller = FailoverPoller()
    zk_watcher = TopicZkWatcher()
    load.start()
    poller.start()
    zk_watcher.start()
    time.sleep(0.4)

    t0 = time.time()
    code, body = http(
        "POST",
        f"/v1/projects/{PROJECT}/topics/{TOPIC}/failover",
        {"sourceRegion": "region-a", "targetRegion": "region-b", "waitForReplicationLagToClear": False},
        note="trigger failover",
    )
    if code != 200:
        load.halt()
        poller.halt()
        zk_watcher.halt()
        log(f"ERROR trigger failed {code} {body}")
        return 1

    done, last = wait_failover_done(timeout=120)
    zt = zk_get_topic(TOPIC)
    snapshot_topic("after failover (COMPLETED)", zt)
    load.halt()
    poller.halt()
    zk_watcher.halt()

    forward_ok = (
        done
        and producing_region(zt) == "region-b"
        and zt.get("topicState") == "Producing"
    )
    log(
        f"RESULT {'PASS' if forward_ok else 'FAIL'} | elapsed={time.time()-t0:.1f}s "
        f"producing={producing_region(zt)} state={zt.get('topicState')} counts={load.counts} last_get={last}"
    )

    write_timeline(load, zt, forward_ok)
    return 0 if forward_ok else 1


if __name__ == "__main__":
    sys.exit(main())
