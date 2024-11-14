// /messages/internal/metrics/messages_metrics.go
package metrics

import (
	observability "github.com/goletan/observability/pkg"
	"github.com/prometheus/client_golang/prometheus"
)

type MessagesMetrics struct{}

// Messages Metrics: Track production and consumption of messages
var (
	MessagesProduced = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "goletan",
			Subsystem: "messages",
			Name:      "produced_total",
			Help:      "Total number of messages produced.",
		},
		[]string{"event_type", "status", "partition"},
	)
	MessagesConsumed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "goletan",
			Subsystem: "messages",
			Name:      "consumed_total",
			Help:      "Total number of messages consumed.",
		},
		[]string{"event_type", "status", "partition"},
	)
)

func InitMetrics(observer *observability.Observability) {
	observer.Metrics.Register(&MessagesMetrics{})
}

func (em *MessagesMetrics) Register() error {
	if err := prometheus.Register(MessagesProduced); err != nil {
		return err
	}

	if err := prometheus.Register(MessagesConsumed); err != nil {
		return err
	}

	return nil
}

// IncrementMessagesProduced increments the Kafka produced message counter
func IncrementMessagesProduced(eventType, status, partition string) {
	MessagesProduced.WithLabelValues(eventType, status, partition).Inc()
}

// IncrementMessagesConsumed increments the Kafka consumed message counter
func IncrementMessagesConsumed(eventType, status, partition string) {
	MessagesConsumed.WithLabelValues(eventType, status, partition).Inc()
}
