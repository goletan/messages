// /messages/kafka/producer.go
package kafka

import (
	"context"
	"time"

	"github.com/goletan/messages/types"
	segmentio "github.com/segmentio/kafka-go"
	segmentio_compress "github.com/segmentio/kafka-go/compress"
	"go.uber.org/zap"
)

// Producer manages producing messages to Kafka.
type Producer struct {
	Writer      *segmentio.Writer
	logger      *zap.Logger
	batch       []segmentio.Message
	batchSize   int
	Retries     int
	Compression string
}

// NewProducer creates a new Kafka producer.
func NewProducer(cfg *types.MessageConfig, log *zap.Logger) *Producer {
	writerConfig := segmentio.WriterConfig{
		Brokers:          cfg.Kafka.Brokers,
		Topic:            cfg.Kafka.Topic,
		BatchSize:        cfg.Kafka.BatchSize,
		CompressionCodec: mapCompressionCodec(cfg.Kafka.Compression),
		WriteTimeout:     time.Duration(cfg.Kafka.Timeout) * time.Second,
	}

	writerConfig.WriteTimeout = time.Duration(cfg.Kafka.Timeout) * time.Second

	return &Producer{
		Writer:    segmentio.NewWriter(writerConfig),
		logger:    log,
		batchSize: cfg.Kafka.BatchSize,
	}
}

// SendMessage sends a message to the Kafka topic, batching messages to improve throughput.
func (p *Producer) SendMessage(ctx context.Context, key, value string, backoff time.Duration) error {
	for i := 0; i <= p.Retries; i++ {
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
	return ctx.Err()
}

// Flush sends any remaining messages in the batch.
func (p *Producer) Flush(ctx context.Context) error {
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
func (p *Producer) Close() error {
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

func mapCompressionCodec(codec string) segmentio_compress.Codec {
	switch codec {
	case "gzip":
		return segmentio_compress.Gzip.Codec()
	case "snappy":
		return segmentio_compress.Snappy.Codec()
	case "lz4":
		return segmentio_compress.Lz4.Codec()
	case "zstd":
		return segmentio_compress.Zstd.Codec()
	default:
		return nil // No compression by default
	}
}
