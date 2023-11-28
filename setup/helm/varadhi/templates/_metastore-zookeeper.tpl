{{/*
Zookeeper configuration as metastore for Varadhi.
*/}}

{{- define "configMap.metastore.zookeeper" -}}
zookeeperOptions:
  {{- if .Values.zkDeployment.enabled }}
  connectUrl: {{ template "varadhi.name" . }}-{{ .Values.zkDeployment.name }}:2181
  {{- else }}
  connectUrl: {{ .Values.metastore.zookeeper.connectUrl }}
  {{- end }}
  sessionTimeout: {{ .Values.metastore.zookeeper.sessionTimeout }}
  connectTimeout: {{ .Values.metastore.zookeeper.connectTimeout }}
{{- end }}
