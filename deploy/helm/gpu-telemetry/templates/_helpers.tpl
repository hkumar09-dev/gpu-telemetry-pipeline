{{- define "gpu-telemetry.labels" -}}
app.kubernetes.io/name: gpu-telemetry
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: gpu-telemetry
{{- end }}

{{- define "gpu-telemetry.selectorLabels" -}}
app.kubernetes.io/name: gpu-telemetry
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "gpu-telemetry.image" -}}
{{ printf "%s/%s:%s" .root.Values.image.repository .service .root.Values.image.tag }}
{{- end }}

{{- define "gpu-telemetry.podSecurity" -}}
securityContext:
  runAsNonRoot: true
  runAsUser: 65532
  runAsGroup: 65532
  fsGroup: 65532
  seccompProfile:
    type: RuntimeDefault
{{- end }}

{{- define "gpu-telemetry.containerSecurity" -}}
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities:
    drop: ["ALL"]
{{- end }}
