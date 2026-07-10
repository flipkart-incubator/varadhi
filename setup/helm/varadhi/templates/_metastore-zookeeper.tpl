{{- define "configMap.metastore.globalZookeeperStore" -}}
globalZookeeperOptions:
  connectUrl: {{ required "metastore.globalZookeeperStore.connectUrl is required" .Values.metastore.globalZookeeperStore.connectUrl }}
  sessionTimeoutMs: {{ .Values.metastore.globalZookeeperStore.sessionTimeoutMs }}
  connectTimeoutMs: {{ .Values.metastore.globalZookeeperStore.connectTimeoutMs }}
{{- end }}

{{- define "configMap.metastore.localZookeeperStore" -}}
localZookeeperOptions:
  connectUrl: {{ required "metastore.localZookeeperStore.connectUrl is required" .Values.metastore.localZookeeperStore.connectUrl }}
  sessionTimeoutMs: {{ .Values.metastore.localZookeeperStore.sessionTimeoutMs }}
  connectTimeoutMs: {{ .Values.metastore.localZookeeperStore.connectTimeoutMs }}
{{- end }}
