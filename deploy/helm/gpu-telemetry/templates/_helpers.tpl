{{- define "gpu-telemetry.labels" -}}
app.kubernetes.io/name: gpu-telemetry
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
