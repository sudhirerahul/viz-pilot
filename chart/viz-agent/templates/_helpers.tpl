{{- define "viz-agent.name" -}}
{{- default "viz-agent" .Chart.Name -}}
{{- end -}}

{{- define "viz-agent.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "viz-agent.name" .) | trunc 63 -}}
{{- end -}}

{{- define "viz-agent.chart" -}}
{{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}
