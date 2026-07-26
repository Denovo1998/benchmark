#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "benchmark.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "benchmark.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "benchmark.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "benchmark.image" -}}
{{- if .Values.imageDigest -}}
{{- if not (regexMatch "^sha256:[0-9a-fA-F]{64}$" .Values.imageDigest) -}}
{{- fail "imageDigest must be an OCI sha256 digest" -}}
{{- end -}}
{{- printf "%s@%s" .Values.image .Values.imageDigest -}}
{{- else -}}
{{- .Values.image -}}
{{- end -}}
{{- end -}}

{{- define "workers" -}}
{{- $nodeCount := .values.numWorkers | int }}
  {{- range $index0 := until $nodeCount -}}
    {{- $index1 := $index0 | add1 -}}
http://{{ $.release.Name }}-worker-{{ $index0 }}.{{ $.release.Name }}-worker:8080{{ if ne $index1 $nodeCount }},{{ end }}
  {{- end -}}
{{- end -}}
