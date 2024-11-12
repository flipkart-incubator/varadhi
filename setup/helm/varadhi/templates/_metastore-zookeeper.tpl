{{/*
Zookeeper configuration as metastore for Varadhi.
*/}}

{{- define "configMap.metastore.zookeeper" -}}
zookeeperOptions:
  connectUrl: {{ .Values.metastore.zookeeper.connectUrl }}
  sessionTimeoutMs: {{ .Values.metastore.zookeeper.sessionTimeoutMs }}
  connectTimeoutMs: {{ .Values.metastore.zookeeper.connectTimeoutMs }}
{{- end }}
