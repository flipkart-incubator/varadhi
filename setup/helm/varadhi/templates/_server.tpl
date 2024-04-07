{{/*
Varadhi Server configuration.
*/}}
{{- define "configMap.varadhi.server" -}}
components:{{- range .Values.varadhi.app.components }}
  - {{.}} {{- end }}

{{ with .Values.varadhi.app.restOptions }}
restOptions:
  {{- toYaml . | nindent 2 }}
{{- end }}

{{ with .Values.varadhi.app.producerOptions }}
producerOptions:
  {{- toYaml . | nindent 2 }}
{{- end }}

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

authenticationEnabled: {{ .Values.varadhi.app.authenticationEnabled }}
{{- with .Values.varadhi.app.authentication }}
authentication:
  {{- toYaml . | nindent 2 }}
{{- end }}

authorizationEnabled: {{ .Values.varadhi.app.authorizationEnabled }}
authorization:
  superUsers: {{ .Values.varadhi.app.authorization.superUsers }}
  providerClassName: {{ .Values.varadhi.app.authorization.providerClassName }}
  configFile: {{ .Values.deployment.configMountPath }}/authzProvider.yml

messagingStackOptions:
  providerClassName: "com.flipkart.varadhi.pulsar.PulsarStackProvider"
  configFile: {{ .Values.deployment.configMountPath }}/messaging.yml

metaStoreOptions:
  providerClassName: "com.flipkart.varadhi.db.ZookeeperProvider"
  configFile: {{ .Values.deployment.configMountPath }}/metastore.yml

{{ with .Values.varadhi.app.featureFlags }}
featureFlags:
  {{- toYaml . | nindent 2 }}
{{- end }}

{{ template "configMap.metastore.zookeeper" . }}

{{ with .Values.varadhi.app.nodeResourcesOverride }}
nodeResourcesOverride:
  {{- toYaml . | nindent 2 }}
{{- end }}

nodeId: {{ .Values.varadhi.app.nodeId }}
{{- end }}
