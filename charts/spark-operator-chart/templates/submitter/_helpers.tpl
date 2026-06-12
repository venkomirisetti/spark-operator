{{/*
Copyright 2024 The Kubeflow authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}}

{{/*
Submitter service image
*/}}
{{- define "spark-operator.submitter.image" -}}
{{ printf "%s:%s" .Values.submitter.image.repository .Values.submitter.image.tag }}
{{- end -}}

{{/*
Create the name of submitter component
*/}}
{{- define "spark-operator.submitter.name" -}}
{{- include "spark-operator.fullname" . }}-submitter
{{- end -}}

{{/*
Common labels for the submitter
*/}}
{{- define "spark-operator.submitter.labels" -}}
{{ include "spark-operator.labels" . }}
app.kubernetes.io/component: submitter
{{- end -}}

{{/*
Selector labels for the submitter
*/}}
{{- define "spark-operator.submitter.selectorLabels" -}}
{{ include "spark-operator.selectorLabels" . }}
app.kubernetes.io/component: submitter
{{- end -}}

{{/*
Create the name of service account to be used by submitter
*/}}
{{- define "spark-operator.submitter.serviceAccountName" -}}
{{- if .Values.submitter.serviceAccount.create -}}
{{ .Values.submitter.serviceAccount.name | default (include "spark-operator.submitter.name" .) }}
{{- else -}}
{{ .Values.submitter.serviceAccount.name | default "default" }}
{{- end -}}
{{- end -}}

{{/*
Create the name of the deployment to be used by submitter
*/}}
{{- define "spark-operator.submitter.deploymentName" -}}
{{ include "spark-operator.submitter.name" . }}
{{- end -}}

{{/*
Create the name of the service to be used by submitter
*/}}
{{- define "spark-operator.submitter.serviceName" -}}
{{ include "spark-operator.submitter.name" . }}-svc
{{- end -}}

{{/*
Create the name of the pod disruption budget to be used by submitter
*/}}
{{- define "spark-operator.submitter.podDisruptionBudgetName" -}}
{{ include "spark-operator.submitter.name" . }}-pdb
{{- end -}}

{{/*
Create the full submit endpoint URL from the in-cluster service FQDN, port, and submit path.
*/}}
{{- define "spark-operator.submitter.url" -}}
{{- if .Values.submitter.tls.enabled -}}
https://{{ include "spark-operator.submitter.serviceName" . }}.{{ .Release.Namespace }}.svc.cluster.local:{{ .Values.submitter.port }}{{ .Values.submitter.submitPath }}
{{- else -}}
http://{{ include "spark-operator.submitter.serviceName" . }}.{{ .Release.Namespace }}.svc.cluster.local:{{ .Values.submitter.port }}{{ .Values.submitter.submitPath }}
{{- end -}}
{{- end -}}

{{/*
Whether submitter TLS is enabled.
*/}}
{{- define "spark-operator.submitter.tlsEnabled" -}}
{{- if and .Values.submitter.enable .Values.submitter.tls.enabled -}}true{{- end -}}
{{- end -}}

