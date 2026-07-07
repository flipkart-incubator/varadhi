{{/*
In-cluster ZooKeeper ensemble connect string for ZOOKEEPER_SERVERS.
Only used when zkDeployment.enabled (see zookeeper-statefullset.yaml).
*/}}
{{- define "varadhi.zookeeper.connectUrl" -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- $serverName :=  .Values.zkDeployment.name -}}
{{- $namespace := .Release.Namespace -}}
{{- $serviceName := .Values.zkDeployment.name -}}
{{- $clusterDomain := .Values.clusterDomain -}}
{{ range $i, $e := until (int .Values.zkDeployment.replicaCount) }}{{ if ne $i 0 }},{{ end }}{{ $name }}-{{ $serverName }}-{{ $i }}.{{ $name }}-{{ $serviceName }}.{{ $namespace }}.svc.{{ $clusterDomain }}{{ end }}
{{- end }}
