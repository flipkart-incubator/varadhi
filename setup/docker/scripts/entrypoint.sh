#!/bin/bash
set -x

# Modify config files based on environment variables and exec Varadhi Application.
if [[ ! -z "$ZOOKEEPER_SERVERS" ]]; then
    sed -i  "s|127.0.0.1:2181|$ZOOKEEPER_SERVERS|" /etc/varadhi/metastore.yml
    sed -i  "s|127.0.0.1:2181|$ZOOKEEPER_SERVERS|" /etc/varadhi/configuration.yml
fi
if [[ ! -z "$PULSAR_URL" ]]; then
    sed -i  "s|http://127.0.0.1:8080|$PULSAR_URL|" /etc/varadhi/messaging.yml
fi

# JAVA JMX options
JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.port=9990"
JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.authenticate=false"
JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.ssl=false"
JAVA_OPTS="$JAVA_OPTS -Dlog4j2.configurationFile=/etc/varadhi/log4j2.xml"

exec java -cp ./*:dependencies/* $JAVA_OPTS com.flipkart.varadhi.VaradhiApplication /etc/varadhi/configuration.yml
