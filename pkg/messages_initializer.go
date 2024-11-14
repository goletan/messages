// /messages/pkg/messages_initializer.go
package messages

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/goletan/messages/internal/metrics"
	"github.com/goletan/messages/internal/types"
	observability "github.com/goletan/observability/pkg"
)

// Init initializes Kafka consumer and producer, setting up graceful shutdown.
func Init(cfg *types.MessageConfig, obs *observability.Observability) (context.Context, context.CancelFunc, *Consumer, *Producer) {
	// Initialize metrics collection
	metrics.InitMetrics(obs)

	// Create context with cancel for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// Handle system interrupts for graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalChan
		obs.Logger.Info("Received shutdown signal")
		cancel()
	}()

	// Create Kafka consumer and producer
	consumer := NewConsumer(cfg, obs)
	producer := NewProducer(cfg, obs)

	return ctx, cancel, consumer, producer
}
