#!/bin/bash
set -e
set -x

# Modify config files based on environment variables and exec Varadhi Application.

if [[ ! -z "$ZK_URL" ]]; then
    sed -i  "s|127.0.0.1:2181|$ZK_URL|" /etc/varadhi/zkConfig.yml
fi
if [[ ! -z "$PULSAR_URL" ]]; then
    sed -i  "s|http://127.0.0.1:8080|$PULSAR_URL|" /etc/varadhi/pulsarConfig.yml
fi

# update node id index, based on replica index.
HOSTNAME="$(hostname -s)"
echo "Fixing the nodeid for the host $HOSTNAME."
if [[ $HOSTNAME =~ (.*)-([0-9]+)$ ]]; then
    ORD=${BASH_REMATCH[2]}
    sed -i "/nodeId:/s/$/$ORD/" /etc/varadhi/configuration.yml
else
    echo "Failed to get index from hostname $HOSTNAME"
    exit 1
fi

# JAVA JMX options
JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.port=9990"
JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.authenticate=false"
JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.ssl=false"

exec java -cp ./*:dependencies/* $JAVA_OPTS com.flipkart.varadhi.VaradhiApplication /etc/varadhi/configuration.yml
