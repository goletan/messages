// /messages/kafka/consumer.go
package kafka

import (
	"context"

	"github.com/goletan/messages/types"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// KafkaConsumer manages consuming messages from Kafka.
type KafkaConsumer struct {
	Reader *kafka.Reader
	logger *zap.Logger
}

// NewKafkaConsumer creates a new Kafka consumer.
func NewKafkaConsumer(cfg *types.MessageConfig, log *zap.Logger) *KafkaConsumer {
	return &KafkaConsumer{
		Reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: cfg.Kafka.Brokers,
			Topic:   cfg.Kafka.Topic,
			GroupID: cfg.Kafka.GroupID,
		}),
		logger: log,
	}
}

// ReadMessage reads a message from the Kafka topic.
func (c *KafkaConsumer) ReadMessage(ctx context.Context) (string, string, error) {
	msg, err := c.Reader.ReadMessage(ctx)
	if err != nil {
		c.logger.Error("Failed to read message from Kafka", zap.Error(err), zap.String("topic", c.Reader.Config().Topic))
		return "", "", err
	}

	c.logger.Info("Message received", zap.String("topic", c.Reader.Config().Topic), zap.ByteString("key", msg.Key), zap.ByteString("value", msg.Value))
	return string(msg.Key), string(msg.Value), nil
}

// Close closes the Kafka consumer connection.
func (c *KafkaConsumer) Close() error {
	if err := c.Reader.Close(); err != nil {
		c.logger.Error("Failed to close Kafka consumer", zap.Error(err))
		return err
	}

	c.logger.Info("Kafka consumer closed successfully")
	return nil
}
