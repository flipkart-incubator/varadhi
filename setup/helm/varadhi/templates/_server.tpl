{{/*
Varadhi Server configuration.
*/}}
{{- define "configMap.varadhi.server" -}}
{{ with .Values.varadhi.app.vertxOptions }}
vertxOptions:
  {{- toYaml . | nindent 2 }}
{{- end }}

{{ with .Values.varadhi.app.verticleDeploymentOptions }}
verticleDeploymentOptions:
  {{- toYaml . | nindent 2 }}
{{- end }}

{{ with .Values.varadhi.app.httpServerOptions }}
httpServerOptions:
  {{- toYaml . | nindent 2 }}
{{- end }}

{{ with .Values.varadhi.app.varadhiOptions }}
varadhiOptions:
  {{- toYaml . | nindent 2 }}
{{- end }}

authenticationEnabled: {{ .Values.varadhi.app.authenticationEnabled }}
{{- with .Values.varadhi.app.authentication }}
authentication:
  {{- toYaml . | nindent 2 }}
{{- end }}

authorizationEnabled: {{ .Values.varadhi.app.authorizationEnabled }}
authorization:
  superUsers: {{ .Values.varadhi.app.authorization.superUsers }}
  providerClassName: {{ .Values.varadhi.app.authorization.providerClassName }}
  configFile: {{ .Values.deployment.configMountPath }}/{{ .Values.authzProvider.configFileName }}

messagingStackOptions:
  providerClassName: "com.flipkart.varadhi.pulsar.PulsarStackProvider"
  configFile: {{ .Values.deployment.configMountPath }}/{{ .Values.messaging.configFileName }}

metaStoreOptions:
  providerClassName: "com.flipkart.varadhi.db.ZookeeperProvider"
  configFile: {{ .Values.deployment.configMountPath }}/{{ .Values.metastore.configFileName }}
{{- end }}
