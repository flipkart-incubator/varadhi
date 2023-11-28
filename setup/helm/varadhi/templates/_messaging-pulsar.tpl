{{/*
Pulsar configuration as underlying message tech stack for Varadhi.
*/}}

{{- define "configMap.messaging.pulsar" -}}
pulsarAdminOptions:
  serviceHttpUrl: {{ .Values.messaging.pulsar.adminOptions.serviceHttpUrl }}
  connectionTimeoutMs: {{ .Values.messaging.pulsar.adminOptions.connectionTimeoutMs }}
  readTimeoutMs: {{ .Values.messaging.pulsar.adminOptions.readTimeoutMs }}
  requestTimeoutMs: {{ .Values.messaging.pulsar.adminOptions.requestTimeoutMs }}
pulsarClientOptions:
  serviceUrl: {{ .Values.messaging.pulsar.clientOptions.serviceUrl }}
  keepAliveIntervalSecs: {{ .Values.messaging.pulsar.clientOptions.keepAliveIntervalSecs }}
  ioThreads: {{ .Values.messaging.pulsar.clientOptions.ioThreads }}
  connectionsPerBroker: {{ .Values.messaging.pulsar.clientOptions.connectionsPerBroker }}
  maxConcurrentLookupRequests: {{ .Values.messaging.pulsar.clientOptions.maxConcurrentLookupRequests }}
  maxLookupRequests: {{ .Values.messaging.pulsar.clientOptions.maxLookupRequests }}
  maxLookupRedirects: {{ .Values.messaging.pulsar.clientOptions.maxLookupRedirects }}
  maxNumberOfRejectedRequestPerConnection: {{ .Values.messaging.pulsar.clientOptions.maxNumberOfRejectedRequestPerConnection }}
  memoryLimit: {{ .Values.messaging.pulsar.clientOptions.memoryLimit }}
  operationTimeoutMs: {{ .Values.messaging.pulsar.clientOptions.operationTimeoutMs }}
  connectionTimeoutMs: {{ .Values.messaging.pulsar.clientOptions.connectionTimeoutMs }}
  lookupTimeoutMs: {{ .Values.messaging.pulsar.clientOptions.lookupTimeoutMs }}
  initialBackoffIntervalMs: {{ .Values.messaging.pulsar.clientOptions.initialBackoffIntervalMs }}
  maxBackoffIntervalMs: {{ .Values.messaging.pulsar.clientOptions.maxBackoffIntervalMs }}
{{- end }}
