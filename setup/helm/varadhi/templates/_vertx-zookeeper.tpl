{{/*
Vert.x cluster manager ZooKeeper connection (configuration.yml vertxZookeeperOptions).
*/}}
{{- define "configMap.varadhi.vertxZookeeperStore" -}}
vertxZookeeperOptions:
  connectUrl: {{ required "varadhi.app.vertxZookeeperStore.connectUrl is required" .Values.varadhi.app.vertxZookeeperStore.connectUrl }}
  sessionTimeoutMs: {{ .Values.varadhi.app.vertxZookeeperStore.sessionTimeoutMs }}
  connectTimeoutMs: {{ .Values.varadhi.app.vertxZookeeperStore.connectTimeoutMs }}
{{- end }}
