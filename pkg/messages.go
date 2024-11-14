// messages/messages.go
package messages

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// ProcessMessages processes messages from the Kafka consumer and handles them.
func ProcessMessages(ctx context.Context, consumer *Consumer, producer *Producer, logger *zap.Logger) {
	go func() {
		batch := []kafka.Message{}
		batchSize := producer.Writer.BatchSize
		batchInterval := time.Second * 5
		batchTicker := time.NewTicker(batchInterval)
		defer batchTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				logger.Info("Stopping message processing")
				return
			case <-batchTicker.C:
				if len(batch) > 0 {
					err := producer.Writer.WriteMessages(ctx, batch...)
					if err != nil {
						logger.Error("Failed to send batch of messages to Kafka", zap.Error(err))
					} else {
						logger.Info("Batch of messages sent successfully", zap.Int("batchSize", len(batch)))
					}
					batch = batch[:0] // Reset batch
				}
			default:
				key, value, err := consumer.ReadMessage(ctx)
				if err != nil {
					logger.Error("Error reading message", zap.Error(err))
					continue
				}

				msg := kafka.Message{
					Key:   []byte(key),
					Value: []byte(value),
				}

				batch = append(batch, msg)

				// Send batch if batch size is reached
				if len(batch) >= batchSize {
					err := producer.Writer.WriteMessages(ctx, batch...)
					if err != nil {
						logger.Error("Failed to send batch of messages to Kafka", zap.Error(err))
					} else {
						logger.Info("Batch of messages sent successfully", zap.Int("batchSize", len(batch)))
					}
					batch = batch[:0] // Reset batch
				}
			}
		}
	}()
}

// SendPublicMessage allows other Nemetons to send messages through Kafka.
func SendPublicMessage(producer *Producer, ctx context.Context, key, value string, retries int, backoff time.Duration) error {
	if retries > 0 {
		return producer.SendMessageWithRetry(ctx, key, value, retries, backoff)
	}
	return producer.SendMessage(ctx, key, value)
}
