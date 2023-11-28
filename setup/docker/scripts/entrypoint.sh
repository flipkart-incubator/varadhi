#!/bin/bash
set -e
set -x

# Modify config files based on environment variables and invoke Varadhi server.

if [[ ! -z "$ZK_URL" ]]; then
    sed -i  "s|127.0.0.1:2181|$ZK_URL|" /etc/varadhi/zkConfig.yml
fi
if [[ ! -z "$PULSAR_URL" ]]; then
    sed -i  "s|http://127.0.0.1:8080|$PULSAR_URL|" /etc/varadhi/pulsarConfig.yml
fi
exec java -cp ./*:dependencies/* com.flipkart.varadhi.Server /etc/varadhi/serverConfig.yml
