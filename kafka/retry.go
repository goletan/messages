// /messages/kafka/retry.go
package kafka

import (
	"context"
	"time"

	"fmt"

	"github.com/goletan/observability"
	segmentio "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// SendMessageWithRetry sends a message with retry logic to improve resilience.
func (p *Producer) SendMessageWithRetry(ctx context.Context, key, value string, retries int, backoff time.Duration) error {
	for i := 0; i <= retries; i++ {
		select {
		case <-ctx.Done():
			p.logger.Warn("Context canceled before sending message", zap.String("topic", p.Writer.Topic))
			return ctx.Err()
		default:
		}

		// Attempt to send the message
		msg := segmentio.Message{
			Key:   []byte(key),
			Value: []byte(value),
		}
		err := p.Writer.WriteMessages(ctx, msg)
		if err == nil {
			p.logger.Info("Message sent successfully", zap.String("topic", p.Writer.Topic), zap.ByteString("key", msg.Key))
			return nil
		}

		// Log the failure and retry if there are attempts left
		p.logger.Error("Failed to send message to Kafka", zap.Error(err), zap.String("topic", p.Writer.Topic), zap.Int("attempt", i+1))
		time.Sleep(backoff * time.Duration(i+1))
	}

	p.logger.Error("Exhausted all retries for sending message", zap.String("topic", p.Writer.Topic), zap.ByteString("key", []byte(key)))
	return fmt.Errorf("exhausted all retries for sending message: %s", key)
}

// SendMessage sends a message without retries.
func (p *Producer) SendMessage(ctx context.Context, key, value string) error {
	msg := segmentio.Message{
		Key:   []byte(key),
		Value: []byte(value),
	}

	err := p.Writer.WriteMessages(ctx, msg)
	if err != nil {
		p.logger.Error("Failed to send message to Kafka", zap.Error(err), zap.String("topic", p.Writer.Topic))
		return err
	}

	p.logger.Info("Message sent successfully", zap.String("topic", p.Writer.Topic), zap.ByteString("key", msg.Key))
	observability.IncrementKafkaProduced()
	return nil
}
