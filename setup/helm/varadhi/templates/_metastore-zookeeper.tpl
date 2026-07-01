{{- define "configMap.metastore.globalZookeeperStore" -}}
globalZookeeperOptions:
  connectUrl: {{ .Values.metastore.globalZookeeperStore.connectUrl }}
  sessionTimeoutMs: {{ .Values.metastore.globalZookeeperStore.sessionTimeoutMs }}
  connectTimeoutMs: {{ .Values.metastore.globalZookeeperStore.connectTimeoutMs }}
{{- end }}

{{- define "configMap.metastore.localZookeeperStore" -}}
localZookeeperOptions:
  connectUrl: {{ .Values.metastore.localZookeeperStore.connectUrl }}
  sessionTimeoutMs: {{ .Values.metastore.localZookeeperStore.sessionTimeoutMs }}
  connectTimeoutMs: {{ .Values.metastore.localZookeeperStore.connectTimeoutMs }}
{{- end }}
