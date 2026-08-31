{{- define "eventpulse.name" -}}
eventpulse
{{- end -}}

{{- define "eventpulse.labels" -}}
app.kubernetes.io/part-of: eventpulse
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "eventpulse.selectorLabels" -}}
app.kubernetes.io/name: {{ .component }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
