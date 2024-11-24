// /messages/pkg/consumer.go
package messages

import (
	"context"
	"time"

	"github.com/goletan/messages/internal/metrics"
	"github.com/goletan/messages/internal/types"
	observability "github.com/goletan/observability/pkg"
	obsKafka "github.com/goletan/observability/shared/kafka"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Consumer manages consuming messages from Kafka.
type Consumer struct {
	Reader        *kafka.Reader
	observability *observability.Observability
	cfg           *types.MessageConfig
}

// NewConsumer creates a new Kafka consumer.
func NewConsumer(cfg *types.MessageConfig, obs *observability.Observability) *Consumer {
	var startOffset int64 = kafka.LastOffset
	if cfg.Kafka.Offset == "earliest" {
		startOffset = kafka.FirstOffset
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:         cfg.Kafka.Brokers,
		Topic:           cfg.Kafka.Topic,
		GroupID:         cfg.Kafka.GroupID,
		StartOffset:     startOffset,
		MinBytes:        10e3,                   // 10KB
		MaxBytes:        10e6,                   // 10MB
		MaxWait:         500 * time.Millisecond, // Poll interval to reduce CPU usage
		ReadLagInterval: -1,                     // Disable lag reporting for higher performance
		QueueCapacity:   1000,                   // Increase queue capacity for prefetching messages
	}

	return &Consumer{
		Reader:        kafka.NewReader(readerConfig),
		observability: obs,
		cfg:           cfg,
	}
}

// ReadMessage reads a message from the Kafka topic with observability, with retry and DLQ handling.
func (c *Consumer) ReadMessage(ctx context.Context) (string, string, error) {
	msg, err := c.Reader.FetchMessage(ctx)
	if err != nil {
		c.observability.Logger.Error("Failed to fetch message from Kafka", zap.Error(err), zap.String("topic", c.Reader.Config().Topic))
		return "", "", err
	}

	// Call the ConsumeMessageWithObservability function to track the message consumption
	err = obsKafka.ConsumeMessageWithObservability(ctx, msg, c.observability.Tracer, c.observability.Logger)
	if err != nil {
		// Retry logic before sending to DLQ
		maxRetries := 3
		for i := 0; i < maxRetries; i++ {
			if retryErr := obsKafka.ConsumeMessageWithObservability(ctx, msg, c.observability.Tracer, c.observability.Logger); retryErr == nil {
				goto CommitMessage // If retry is successful, commit the message
			}
		}

		// Send to DLQ if still failing after retries
		dlqCfg := c.cfg.DLQ
		dlqProducer := NewProducer(&types.MessageConfig{
			Kafka: struct {
				Brokers     []string `mapstructure:"brokers"`
				Topic       string   `mapstructure:"topic"`
				GroupID     string   `mapstructure:"group_id"`
				BatchSize   int      `mapstructure:"batch_size"`
				Retries     int      `mapstructure:"retries"`
				Timeout     int      `mapstructure:"timeout"`
				Compression string   `mapstructure:"compression"`
				Offset      string   `mapstructure:"offset"`
			}{
				Brokers:     dlqCfg.Brokers,
				Topic:       dlqCfg.Topic,
				Compression: dlqCfg.Compression,
			},
		}, c.observability)
		defer dlqProducer.Close()

		dlqMsg := kafka.Message{
			Topic: c.cfg.DLQ.Topic,
			Key:   msg.Key,
			Value: msg.Value,
		}

		// Now, actually send the DLQ message
		err = obsKafka.ProduceMessageWithObservability(context.Background(), dlqProducer.Writer, dlqMsg, c.observability.Tracer, c.observability.Logger)
		if err != nil {
			c.observability.Logger.Error("Failed to send message to DLQ", zap.Error(err), zap.String("topic", dlqMsg.Topic), zap.ByteString("key", dlqMsg.Key))
			return "", "", err
		}

		c.observability.Logger.Warn("Message sent to DLQ", zap.String("topic", msg.Topic), zap.ByteString("key", msg.Key))
		return "", "", err
	}

CommitMessage:
	// After processing the message, commit the message offset
	if err := c.Reader.CommitMessages(ctx, msg); err != nil {
		c.observability.Logger.Error("Failed to commit message offset", zap.Error(err), zap.String("topic", c.Reader.Config().Topic))
		return "", "", err
	}

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
