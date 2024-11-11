#!/bin/bash
set -x

# If JAVA_DEBUG_PORT is set, then enable remote debugging and use that env as port
if [[ -n "$JAVA_DEBUG_PORT" ]]; then
    JAVA_OPTS="$JAVA_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:$JAVA_DEBUG_PORT"
fi

# JAVA JMX options
if [[ -n "$JAVA_JMX_PORT" ]]; then
  JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote"
  JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.port=$JAVA_JMX_PORT"
  JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.ssl=false"
  JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.authenticate=false"
  JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.local.only=false"
fi

JAVA_OPTS="$JAVA_OPTS -Dlog4j2.configurationFile=/etc/varadhi/log4j2.xml"

exec java -cp ./*:dependencies/* $JAVA_OPTS com.flipkart.varadhi.VaradhiApplication /etc/varadhi/configuration.yml
