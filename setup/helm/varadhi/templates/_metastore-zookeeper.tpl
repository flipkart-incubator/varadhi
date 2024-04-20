{{/*
Zookeeper configuration as metastore for Varadhi.
*/}}

{{- define "configMap.metastore.zookeeper" -}}
zookeeperOptions:
  connectUrl: {{ template "varadhi.zookeeper.connectUrl" . }}
  sessionTimeoutMs: {{ .Values.metastore.zookeeper.sessionTimeoutMs }}
  connectTimeoutMs: {{ .Values.metastore.zookeeper.connectTimeoutMs }}
{{- end }}
