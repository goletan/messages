// /messages/kafka/consumer.go
package kafka

import (
	"context"

	"github.com/goletan/messages/types"
	segmentio "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Consumer manages consuming messages from Kafka.
type Consumer struct {
	Reader *segmentio.Reader
	logger *zap.Logger
}

// NewConsumer creates a new Kafka consumer.
func NewConsumer(cfg *types.MessageConfig, log *zap.Logger) *Consumer {
	var startOffset int64
	startOffset = segmentio.LastOffset
	if cfg.Kafka.Offset == "earliest" {
		startOffset = segmentio.FirstOffset
	}

	readerConfig := segmentio.ReaderConfig{
		Brokers:     cfg.Kafka.Brokers,
		Topic:       cfg.Kafka.Topic,
		GroupID:     cfg.Kafka.GroupID,
		StartOffset: startOffset,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
	}

	return &Consumer{
		Reader: segmentio.NewReader(readerConfig),
		logger: log,
	}
}

// ReadMessage reads a message from the Kafka topic.
func (c *Consumer) ReadMessage(ctx context.Context) (string, string, error) {
	msg, err := c.Reader.ReadMessage(ctx)
	if err != nil {
		c.logger.Error("Failed to read message from Kafka", zap.Error(err), zap.String("topic", c.Reader.Config().Topic))
		return "", "", err
	}

	c.logger.Info("Message received", zap.String("topic", c.Reader.Config().Topic), zap.ByteString("key", msg.Key), zap.ByteString("value", msg.Value))
	observability.IncrementKafkaConsumed()
	return string(msg.Key), string(msg.Value), nil
}

// Close closes the Kafka consumer connection.
func (c *Consumer) Close() error {
	if err := c.Reader.Close(); err != nil {
		c.logger.Error("Failed to close Kafka consumer", zap.Error(err))
		return err
	}

	c.logger.Info("Kafka consumer closed successfully")
	return nil
}
