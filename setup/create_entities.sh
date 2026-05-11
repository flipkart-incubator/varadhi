#!/usr/bin/env bash

BASE_URL="http://localhost:18488"
MAX_RETRIES=5
RETRY_INTERVAL=1

# 4 params: org, team, project, topic
if [ "$#" -ne 4 ]; then
    echo "Usage: $0 org team project topic"
    exit 1
fi

wait_for_resource() {
  local url=$1
  local name=$2
  for i in $(seq 1 $MAX_RETRIES); do
    STATUS=$(curl -s -o /dev/null -w "%{http_code}" --header 'x_user_id: thanos' "$url")
    if [ "$STATUS" = "200" ]; then
      echo "$name is ready"
      return 0
    fi
    echo "  waiting for $name... (attempt $i/$MAX_RETRIES)"
    sleep $RETRY_INTERVAL
  done
  echo "ERROR: $name not available after $MAX_RETRIES attempts"
  return 1
}

echo "Creating organization $1"
curl --request POST \
  --url "$BASE_URL/v1/orgs/" \
  --header 'x_user_id: thanos' \
  --data '{
	"name": "'"$1"'"
}'

echo ""
echo "Creating team $2"
curl --request POST \
  --url "$BASE_URL/v1/orgs/$1/teams" \
  --header 'x_user_id: thanos' \
  --data '{
	"name": "'"$2"'",
	"org": "'"$1"'"
}'

echo ""
echo "Creating project $3"
curl --request POST \
  --url "$BASE_URL/v1/projects" \
  --header 'x_user_id: thanos' \
  --data '{
	"name": "'"$3"'",
	"org": "'"$1"'",
	"team": "'"$2"'",
	"description": "test project"
}'

echo ""
wait_for_resource "$BASE_URL/v1/projects/$3" "Project($3)" || exit 1

echo ""
echo "Creating topic $4"
curl --request POST \
  --url "$BASE_URL/v1/projects/$3/topics" \
  --header 'x_user_id: thanos' \
  --data '{
	"name": "'"$4"'",
	"version": 0,
	"project": "'"$3"'",
	"grouped": false,
	"capacity": {
		"qps": 10,
		"throughputKBps": 10,
		"readFanOut": 10
	}
}'

echo ""
wait_for_resource "$BASE_URL/v1/projects/$3/topics/$4" "Topic($4)" || exit 1

echo ""
echo "Creating subscription to deliver messages"
curl --request POST \
  --url "$BASE_URL/v1/projects/$3/subscriptions" \
  --header 'x_user_id: thanos' \
  --header 'Content-Type: application/json' \
  --data '{
	"name": "default_subscription",
	"version": 0,
	"project": "'"$3"'",
	"topic": "'"$4"'",
	"topicProject": "'"$3"'",
	"description": "a sample subscription to deliver the messages",
	"grouped": false,
	"endpoint": {
		"uri": "http://host.docker.internal:8000/messages",
		"method": "POST",
		"contentType": "application/json",
		"connectTimeoutMs": 5000,
		"requestTimeoutMs": 5000,
		"http2Supported": false,
		"protocol": "HTTP1_1"
	},
	"retryPolicy": {
		"retryCodes": [
			{
				"from": 300,
				"to": 600
			}
		],
		"backoffType": "LINEAR",
		"minBackoff": 1,
		"maxBackoff": 60,
		"multiplier": 2,
		"retryAttempts": 3
	},
	"consumptionPolicy": {
		"maxInFlightMessages": 10,
		"maxParallelism": 2,
		"maxRecoveryAllocation": 0.5,
		"dltRecoveryPreferred": false,
		"maxErrorThreshold": 10,
		"throttlePolicy": {
			"factor": 0.8,
			"waitSeconds": 1,
			"pingSeconds": 1,
			"stopAfterSeconds": 60
		}
	},
	"targetClientIds": {
      "default": "default-client"
    }
}'

echo ""
wait_for_resource "$BASE_URL/v1/projects/$3/subscriptions/default_subscription" "Subscription(default_subscription)" || exit 1

echo ""
echo "Starting the subscription"
curl --request POST \
  --url "$BASE_URL/v1/projects/$3/subscriptions/default_subscription/start" \
  --header 'x_user_id: thanos'
echo ""
