{{/*
Default Authz provider (Server) configuration.
*/}}
{{- define "configMap.authzProvider.default" -}}
{{- with .Values.authzProvider.default }}
{{- toYaml . }}
{{- end }}
{{- end}}
