package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const ns = "gpu_telemetry"

// Registry is the process-wide gatherer served at GET /metrics.
var Registry = prometheus.NewRegistry()

var (
	MQPublished = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "mq", Name: "messages_published_total",
		Help: "Messages accepted by the broker.",
	})
	MQConsumed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "mq", Name: "messages_consumed_total",
		Help: "Messages delivered to a consumer (in-flight).",
	})
	MQAcknowledged = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "mq", Name: "messages_acknowledged_total",
		Help: "Messages successfully acked.",
	})
	MQRetried = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "mq", Name: "messages_retried_total",
		Help: "Messages requeued after nack or ack timeout.",
	})
	MQPublishFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "mq", Name: "publish_failures_total",
		Help: "Publish attempts rejected (backpressure, closed, context).",
	})
	MQConsumerFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "mq", Name: "consumer_failures_total",
		Help: "Consume calls that failed (not idle timeout).",
	})
	MQQueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Subsystem: "mq", Name: "queue_depth",
		Help: "Queued (not in-flight) messages per topic.",
	}, []string{"topic"})

	StreamerProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "streamer", Name: "records_processed_total",
		Help: "CSV rows successfully published.",
	})
	StreamerPublishFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "streamer", Name: "publish_failures_total",
		Help: "Streamer publish errors.",
	})

	CollectorConsumed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "collector", Name: "records_consumed_total",
		Help: "Deliveries received from the broker.",
	})
	CollectorPersisted = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "collector", Name: "records_persisted_total",
		Help: "Deliveries written to the gateway.",
	})
	CollectorPersistFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "collector", Name: "persistence_failures_total",
		Help: "Gateway write failures.",
	})

	HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "http", Name: "requests_total",
		Help: "HTTP requests handled by the gateway.",
	}, []string{"method", "path", "code"})
	HTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Subsystem: "http", Name: "request_duration_seconds",
		Help:    "Gateway request latency.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
	}, []string{"method", "path"})
	HTTPErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Subsystem: "http", Name: "errors_total",
		Help: "HTTP responses with status >= 400.",
	}, []string{"method", "path", "code"})
)

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		MQPublished, MQConsumed, MQAcknowledged, MQRetried,
		MQPublishFailures, MQConsumerFailures, MQQueueDepth,
		StreamerProcessed, StreamerPublishFailures,
		CollectorConsumed, CollectorPersisted, CollectorPersistFailures,
		HTTPRequests, HTTPDuration, HTTPErrors,
	)
}

func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{Registry: Registry})
}

func SetQueueDepth(topic string, n int) {
	if topic == "" {
		topic = "unknown"
	}
	MQQueueDepth.WithLabelValues(topic).Set(float64(n))
}
