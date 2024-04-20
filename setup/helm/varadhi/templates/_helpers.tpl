{{/*
Expand the name of the chart.
*/}}
{{- define "varadhi.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}


{{/*
Common labels.
*/}}
{{- define "varadhi.commonLabels" -}}
app: {{ template "varadhi.name" . }}
release: {{ .Release.Name }}
{{- end }}

{{/*
Create the match labels.
*/}}
{{- define "varadhi.matchLabels" -}}
app: {{ template "varadhi.name" . }}
release: {{ .Release.Name }}
{{- end }}


{{/*
Selector labels
*/}}
{{- define "varadhi.selectorLabels" -}}
app.kubernetes.io/name: {{ include "varadhi.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Define Zookeeper Connect URL
*/}}
{{- define "varadhi.zookeeper.connectUrl" -}}
{{- if .Values.zkDeployment.enabled -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- $serverName :=  .Values.zkDeployment.name -}}
{{- $namespace := .Release.Namespace -}}
{{- $serviceName := .Values.zkDeployment.name -}}
{{- $clusterDomain := .Values.clusterDomain -}}
{{ range $i, $e := until (int .Values.zkDeployment.replicaCount) }}{{ if ne $i 0 }},{{ end }}{{ $name }}-{{ $serverName }}-{{ $i }}.{{ $name }}-{{ $serviceName }}.{{ $namespace }}.svc.{{ $clusterDomain }}{{ end }}
{{- else -}}
{{- .Values.metastore.zookeeper.connectUrl -}}
{{- end -}}
{{- end }}

