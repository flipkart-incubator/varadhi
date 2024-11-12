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
