// /messages/kafka/producer.go
package kafka

import (
	"context"

	"github.com/goletan/messages/types"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// KafkaProducer manages producing messages to Kafka.
type KafkaProducer struct {
	Writer    *kafka.Writer
	logger    *zap.Logger
	batch     []kafka.Message
	batchSize int
}

// NewKafkaProducer creates a new Kafka producer.
func NewKafkaProducer(cfg *types.MessageConfig, log *zap.Logger) *KafkaProducer {
	return &KafkaProducer{
		Writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers: cfg.Kafka.Brokers,
			Topic:   cfg.Kafka.Topic,
		}),
		logger:    log,
		batchSize: cfg.Kafka.BatchSize,
	}
}

// SendMessage sends a message to the Kafka topic, batching messages to improve throughput.
func (p *KafkaProducer) SendMessage(ctx context.Context, key, value string) error {
	select {
	case <-ctx.Done():
		p.logger.Warn("Context canceled before sending message", zap.String("topic", p.Writer.Topic))
		return ctx.Err()
	default:
	}

	// Add message to batch
	p.batch = append(p.batch, kafka.Message{
		Key:   []byte(key),
		Value: []byte(value),
	})

	// Send batch if batch size is reached
	if len(p.batch) >= p.batchSize {
		err := p.Writer.WriteMessages(ctx, p.batch...)
		if err != nil {
			p.logger.Error("Failed to send batch of messages to Kafka", zap.Error(err), zap.String("topic", p.Writer.Topic))
			return err
		}

		p.logger.Info("Batch of messages sent successfully", zap.String("topic", p.Writer.Topic), zap.Int("batchSize", len(p.batch)))
		p.batch = p.batch[:0] // Reset batch
	}

	return nil
}

// Flush sends any remaining messages in the batch.
func (p *KafkaProducer) Flush(ctx context.Context) error {
	if len(p.batch) == 0 {
		return nil
	}

	err := p.Writer.WriteMessages(ctx, p.batch...)
	if err != nil {
		p.logger.Error("Failed to flush batch of messages to Kafka", zap.Error(err), zap.String("topic", p.Writer.Topic))
		return err
	}

	p.logger.Info("Batch of messages flushed successfully", zap.String("topic", p.Writer.Topic), zap.Int("batchSize", len(p.batch)))
	p.batch = p.batch[:0] // Reset batch
	return nil
}

// Close closes the Kafka producer connection.
func (p *KafkaProducer) Close() error {
	// Flush any remaining messages before closing
	if err := p.Flush(context.Background()); err != nil {
		p.logger.Error("Failed to flush messages during close", zap.Error(err))
		return err
	}

	if err := p.Writer.Close(); err != nil {
		p.logger.Error("Failed to close Kafka producer", zap.Error(err))
		return err
	}

	p.logger.Info("Kafka producer closed successfully")
	return nil
}
