{{- define "configMap.logging.log4j2" -}}
<?xml version="1.0" encoding="UTF-8"?>
<!--
TODO:: This is not being auto picked. Needs to be fixed.
-->
<!--
  Preferring Log4j2 over logback primarily for Async logger feature.
  However detailed evaluation and configuration for Async loggers by default  yet to be done.
 -->
<Configuration status="warn">
  <Appenders>
    <!-- Console appender configuration -->
    <Console name="console" target="SYSTEM_OUT">
      <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n"/>
    </Console>
  </Appenders>
  <Loggers>
    <!-- Root logger referring to console appender -->
    <Root level="{{ .Values.logging.level }}" additivity="false">
      <AppenderRef ref="console"/>
    </Root>
  </Loggers>
</Configuration>
{{- end }}
