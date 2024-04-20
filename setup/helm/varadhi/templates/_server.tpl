{{/*
Varadhi Server configuration.
*/}}
{{- define "configMap.varadhi.server" -}}

{{ with .Values.varadhi.app.member }}
member:
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

{{ with .Values.varadhi.app.deliveryOptions }}
deliveryOptions:
  {{- toYaml . | nindent 2 }}
{{- end }}

{{ with .Values.varadhi.app.httpServerOptions }}
httpServerOptions:
  {{- toYaml . | nindent 2 }}
{{- end }}

{{ with .Values.varadhi.app.restOptions }}
restOptions:
  {{- toYaml . | nindent 2 }}
{{- end }}

{{ with .Values.varadhi.app.producerOptions }}
producerOptions:
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
  configFile: /etc/varadhi/authorizationConfig.yml

messagingStackOptions:
  providerClassName: "com.flipkart.varadhi.pulsar.PulsarStackProvider"
  configFile: /etc/varadhi/messaging.yml

metaStoreOptions:
  providerClassName: "com.flipkart.varadhi.db.ZookeeperProvider"
  configFile: /etc/varadhi/metastore.yml

{{ with .Values.varadhi.app.featureFlags }}
featureFlags:
  {{- toYaml . | nindent 2 }}
{{- end }}

{{ template "configMap.metastore.zookeeper" . }}

{{- end }}
