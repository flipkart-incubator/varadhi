#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:18488}"
ORG="${ORG:-failover_org}"
TEAM="${TEAM:-failover_team}"
PROJECT="${PROJECT:-failover_project}"
TOPIC="${TOPIC:-failover_topic}"
SOURCE_REGION="${SOURCE_REGION:-region-a}"
TARGET_REGION="${TARGET_REGION:-region-b}"
USER_HEADER="${USER_HEADER:-thanos}"
TOPIC_FQN="${PROJECT}.${TOPIC}"
POLL_SECS="${POLL_SECS:-120}"

hdr=(-H "x_user_id: ${USER_HEADER}")

wait_http() {
  local url=$1 name=$2
  for i in $(seq 1 30); do
    if curl -sS --connect-timeout 2 --max-time 5 -o /dev/null -w "%{http_code}" "${hdr[@]}" "$url" | grep -qE '^(200|204)$'; then
      echo "OK: $name"
      return 0
    fi
    sleep 2
  done
  echo "TIMEOUT waiting for $name at $url" >&2
  return 1
}

produce() {
  local msg_id=$1 body=$2
  local code
  code=$(curl -sS -o /tmp/varadhi_produce_resp.json -w "%{http_code}" \
    -X POST "${BASE_URL}/v1/projects/${PROJECT}/topics/${TOPIC}/produce" \
    "${hdr[@]}" \
    -H "X_MESSAGE_ID: ${msg_id}" \
    -H "Content-Type: application/octet-stream" \
    --data-binary "$body")
  echo "produce(${msg_id}) -> HTTP ${code}"
  cat /tmp/varadhi_produce_resp.json || true
  echo
  [[ "$code" == "200" ]]
}

echo "=== 1) Wait for Varadhi ==="
wait_http "${BASE_URL}/v1/health-check" "health-check" || true

echo "=== 2) Register regions ${SOURCE_REGION}, ${TARGET_REGION} ==="
for region in "$SOURCE_REGION" "$TARGET_REGION"; do
  curl -sS "${hdr[@]}" -H "Content-Type: application/json" \
    -X POST "${BASE_URL}/v1/regions" \
    -d "{\"name\":\"${region}\",\"status\":\"AVAILABLE\"}" || true
  echo
done

echo "=== 3) Create org/team/project/topic ==="
bash "$(dirname "$0")/create_entities.sh" "$ORG" "$TEAM" "$PROJECT" "$TOPIC"

echo "=== 4) Seed second produce region in metastore (${TARGET_REGION}) ==="
TOPIC_FQN="$TOPIC_FQN" TARGET_REGION="$TARGET_REGION" \
  ./gradlew :server:test --tests "com.flipkart.varadhi.failover.FailoverTopicSeederTest.seedSecondProduceRegion" -q

echo "=== 5) Scenario 1: normal produce (active=${SOURCE_REGION}) ==="
produce "pre-failover-$(date +%s)" '{"phase":"before-failover"}'

echo "=== 6) Scenario 2: trigger failover ${SOURCE_REGION} -> ${TARGET_REGION} ==="
curl -sS "${hdr[@]}" -H "Content-Type: application/json" \
  -X POST "${BASE_URL}/v1/projects/${PROJECT}/topics/${TOPIC}/failover" \
  -d "{\"sourceRegion\":\"${SOURCE_REGION}\",\"targetRegion\":\"${TARGET_REGION}\",\"waitForReplicationLagToClear\":false}"
echo

echo "=== 7) Poll failover until COMPLETED (max ${POLL_SECS}s) ==="
deadline=$((SECONDS + POLL_SECS))
while (( SECONDS < deadline )); do
  status_json=$(curl -sS "${hdr[@]}" "${BASE_URL}/v1/projects/${PROJECT}/topics/${TOPIC}/failover" || true)
  echo "$status_json"
  if echo "$status_json" | grep -q '"currentStage"[[:space:]]*:[[:space:]]*"COMPLETED"'; then
    echo "FAILOVER COMPLETED"
    break
  fi
  if echo "$status_json" | grep -q '"currentStage"[[:space:]]*:[[:space:]]*"ABORTED"'; then
    echo "FAILOVER ABORTED" >&2
    exit 1
  fi
  sleep 3
done

if ! echo "$status_json" | grep -q '"currentStage"[[:space:]]*:[[:space:]]*"COMPLETED"'; then
  echo "FAILOVER did not complete within ${POLL_SECS}s" >&2
  exit 1
fi

echo "=== 8) Produce after failover (active should be ${TARGET_REGION}) ==="
produce "post-failover-$(date +%s)" '{"phase":"after-failover"}'

echo "=== DONE: both scenarios passed ==="
