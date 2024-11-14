// /messages/pkg/consumer.go
package messages

import (
	"context"

	"github.com/goletan/messages/internal/types"
	observability "github.com/goletan/observability/pkg"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Consumer manages consuming messages from Kafka.
type Consumer struct {
	Reader        *kafka.Reader
	observability *observability.Observability
}

// NewConsumer creates a new Kafka consumer.
func NewConsumer(cfg *types.MessageConfig, obs *observability.Observability) *Consumer {
	var startOffset int64 = kafka.LastOffset
	if cfg.Kafka.Offset == "earliest" {
		startOffset = kafka.FirstOffset
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:     cfg.Kafka.Brokers,
		Topic:       cfg.Kafka.Topic,
		GroupID:     cfg.Kafka.GroupID,
		StartOffset: startOffset,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
	}

	return &Consumer{
		Reader:        kafka.NewReader(readerConfig),
		observability: obs,
	}
}

// ReadMessage reads a message from the Kafka topic.
func (c *Consumer) ReadMessage(ctx context.Context) (string, string, error) {
	msg, err := c.Reader.ReadMessage(ctx)
	if err != nil {
		c.observability.Logger.Error("Failed to read message from Kafka", zap.Error(err), zap.String("topic", c.Reader.Config().Topic))
		return "", "", err
	}

	c.observability.Logger.Info("Message received", zap.String("topic", c.Reader.Config().Topic), zap.ByteString("key", msg.Key), zap.ByteString("value", msg.Value))
	metrics.IncrementMessagesConsumed(msg.Topic, "read", "")
	return string(msg.Key), string(msg.Value), nil
}

// Close closes the Kafka consumer connection.
func (c *Consumer) Close() error {
	if err := c.Reader.Close(); err != nil {
		c.observability.Logger.Error("Failed to close consumer", zap.Error(err))
		return err
	}

	c.observability.Logger.Info("Consumer closed successfully")
	return nil
}
