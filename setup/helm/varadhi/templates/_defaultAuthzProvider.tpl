{{/*
Default Authz provider (Server) configuration.
*/}}
{{- define "configMap.authzProvider.default" -}}
metaStoreOptions:
  providerClassName: {{ .Values.varadhi.app.metaStoreOptions.providerClassName }}
  configFile: /etc/varadhi/metastore.yml

roleDefinitions:
  org.admin:
    roleId: org.admin
    permissions:
      - ORG_CREATE
      - ORG_UPDATE
      - ORG_DELETE
      - ORG_GET
      - ORG_LIST
      - ORG_PROJECT_MIGRATE
      - TEAM_CREATE
      - TEAM_UPDATE
      - TEAM_DELETE
      - TEAM_GET
      - TEAM_LIST
      - PROJECT_CREATE
      - PROJECT_UPDATE
      - PROJECT_DELETE
      - PROJECT_LIST
      - PROJECT_GET
      - TOPIC_CREATE
      - TOPIC_UPDATE
      - TOPIC_DELETE
      - TOPIC_GET
      - TOPIC_LIST
      - TOPIC_CONSUME
      - TOPIC_PRODUCE
      - SUBSCRIPTION_CREATE
      - SUBSCRIPTION_UPDATE
      - SUBSCRIPTION_DELETE
      - SUBSCRIPTION_GET
      - SUBSCRIPTION_LIST
      - SUBSCRIPTION_SEEK
      - IAM_POLICY_SET
      - IAM_POLICY_GET
      - IAM_POLICY_DELETE
      - TOPIC_IAM_POLICY_SET
      - TOPIC_IAM_POLICY_GET
      - TOPIC_IAM_POLICY_DELETE
      - SUBSCRIPTION_IAM_POLICY_SET
      - SUBSCRIPTION_IAM_POLICY_GET
      - SUBSCRIPTION_IAM_POLICY_DELETE
  org.reader:
    roleId: org.reader
    permissions:
      - ORG_GET
      - ORG_LIST
      - TEAM_GET
      - TEAM_LIST
      - PROJECT_GET
      - PROJECT_LIST
      - TOPIC_GET
      - SUBSCRIPTION_GET
      - SUBSCRIPTION_LIST
      - IAM_POLICY_GET
      - TOPIC_IAM_POLICY_GET
      - SUBSCRIPTION_IAM_POLICY_GET
  org.rw:
    roleId: org.rw
    permissions:
      - ORG_CREATE
      - ORG_UPDATE
      - ORG_DELETE
      - ORG_GET
      - ORG_LIST
      - TEAM_CREATE
      - TEAM_UPDATE
      - TEAM_DELETE
      - TEAM_GET
      - TEAM_LIST
      - PROJECT_CREATE
      - PROJECT_UPDATE
      - PROJECT_DELETE
      - PROJECT_LIST
      - PROJECT_GET
      - TOPIC_CREATE
      - TOPIC_UPDATE
      - TOPIC_DELETE
      - TOPIC_GET
      - SUBSCRIPTION_CREATE
      - SUBSCRIPTION_UPDATE
      - SUBSCRIPTION_DELETE
      - SUBSCRIPTION_GET
      - SUBSCRIPTION_LIST
      - IAM_POLICY_GET
      - TOPIC_IAM_POLICY_GET
      - SUBSCRIPTION_IAM_POLICY_GET
  team.admin:
    roleId: team.admin
    permissions:
      - TEAM_CREATE
      - TEAM_UPDATE
      - TEAM_DELETE
      - TEAM_GET
      - TEAM_LIST
      - PROJECT_CREATE
      - PROJECT_UPDATE
      - PROJECT_DELETE
      - PROJECT_LIST
      - PROJECT_GET
      - TOPIC_CREATE
      - TOPIC_UPDATE
      - TOPIC_DELETE
      - TOPIC_GET
      - TOPIC_CONSUME
      - TOPIC_PRODUCE
      - SUBSCRIPTION_CREATE
      - SUBSCRIPTION_UPDATE
      - SUBSCRIPTION_DELETE
      - SUBSCRIPTION_GET
      - SUBSCRIPTION_LIST
      - SUBSCRIPTION_SEEK
      - IAM_POLICY_SET
      - IAM_POLICY_GET
      - IAM_POLICY_DELETE
      - TOPIC_IAM_POLICY_SET
      - TOPIC_IAM_POLICY_GET
      - TOPIC_IAM_POLICY_DELETE
      - SUBSCRIPTION_IAM_POLICY_SET
      - SUBSCRIPTION_IAM_POLICY_GET
      - SUBSCRIPTION_IAM_POLICY_DELETE
  team.reader:
    roleId: team.reader
    permissions:
      - TEAM_GET
      - TEAM_LIST
      - PROJECT_GET
      - PROJECT_LIST
      - TOPIC_GET
      - SUBSCRIPTION_GET
      - SUBSCRIPTION_LIST
      - IAM_POLICY_GET
      - TOPIC_IAM_POLICY_GET
      - SUBSCRIPTION_IAM_POLICY_GET
  team.rw:
    roleId: team.rw
    permissions:
      - TEAM_CREATE
      - TEAM_UPDATE
      - TEAM_DELETE
      - TEAM_GET
      - TEAM_LIST
      - PROJECT_CREATE
      - PROJECT_UPDATE
      - PROJECT_DELETE
      - PROJECT_LIST
      - PROJECT_GET
      - TOPIC_CREATE
      - TOPIC_UPDATE
      - TOPIC_DELETE
      - TOPIC_GET
      - SUBSCRIPTION_CREATE
      - SUBSCRIPTION_UPDATE
      - SUBSCRIPTION_DELETE
      - SUBSCRIPTION_GET
      - SUBSCRIPTION_LIST
      - IAM_POLICY_GET
      - TOPIC_IAM_POLICY_GET
      - SUBSCRIPTION_IAM_POLICY_GET
  project.admin:
    roleId: project.admin
    permissions:
      - PROJECT_CREATE
      - PROJECT_UPDATE
      - PROJECT_DELETE
      - PROJECT_LIST
      - PROJECT_GET
      - TOPIC_CREATE
      - TOPIC_UPDATE
      - TOPIC_DELETE
      - TOPIC_GET
      - TOPIC_CONSUME
      - TOPIC_PRODUCE
      - SUBSCRIPTION_CREATE
      - SUBSCRIPTION_UPDATE
      - SUBSCRIPTION_DELETE
      - SUBSCRIPTION_GET
      - SUBSCRIPTION_LIST
      - SUBSCRIPTION_SEEK
      - IAM_POLICY_SET
      - IAM_POLICY_GET
      - IAM_POLICY_DELETE
      - TOPIC_IAM_POLICY_SET
      - TOPIC_IAM_POLICY_GET
      - TOPIC_IAM_POLICY_DELETE
      - SUBSCRIPTION_IAM_POLICY_SET
      - SUBSCRIPTION_IAM_POLICY_GET
      - SUBSCRIPTION_IAM_POLICY_DELETE
  project.reader:
    roleId: project.reader
    permissions:
      - PROJECT_GET
      - PROJECT_LIST
      - TOPIC_GET
      - SUBSCRIPTION_GET
      - SUBSCRIPTION_LIST
      - IAM_POLICY_GET
      - TOPIC_IAM_POLICY_GET
      - SUBSCRIPTION_IAM_POLICY_GET
  project.rw:
    roleId: project.rw
    permissions:
      - PROJECT_CREATE
      - PROJECT_UPDATE
      - PROJECT_DELETE
      - PROJECT_LIST
      - PROJECT_GET
      - TOPIC_CREATE
      - TOPIC_UPDATE
      - TOPIC_DELETE
      - TOPIC_GET
      - SUBSCRIPTION_CREATE
      - SUBSCRIPTION_UPDATE
      - SUBSCRIPTION_DELETE
      - SUBSCRIPTION_GET
      - SUBSCRIPTION_LIST
      - IAM_POLICY_GET
      - TOPIC_IAM_POLICY_GET
      - SUBSCRIPTION_IAM_POLICY_GET
{{- end}}
