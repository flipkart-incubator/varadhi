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

