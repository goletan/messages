// /messages/producer.go
package messages

import (
	"context"
	"time"

	"github.com/goletan/messages/internal/types"
	observability "github.com/goletan/observability/pkg"
	obsKafka "github.com/goletan/observability/shared/kafka"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"go.uber.org/zap"
)

// Producer manages producing messages to Kafka.
type Producer struct {
	Writer        *kafka.Writer
	observability *observability.Observability
	batch         []kafka.Message
	batchSize     int
	Retries       int
	Compression   string
}

// NewProducer creates a new Kafka producer with configurable retries and timeouts.
func NewProducer(cfg *types.MessageConfig, obs *observability.Observability) *Producer {
	writerConfig := kafka.WriterConfig{
		Brokers:          cfg.Kafka.Brokers,
		Topic:            cfg.Kafka.Topic,
		BatchSize:        cfg.Kafka.BatchSize,
		CompressionCodec: mapCompressionCodec(cfg.Kafka.Compression),
		WriteTimeout:     time.Duration(cfg.Kafka.Timeout) * time.Second,
		Async:            true, // Use async writing for better performance
	}

	return &Producer{
		Writer:        kafka.NewWriter(writerConfig),
		observability: obs,
		batchSize:     cfg.Kafka.BatchSize,
		Retries:       cfg.Kafka.Retries,
	}
}

// Flush sends any remaining messages in the batch.
func (p *Producer) Flush(ctx context.Context) error {
	if len(p.batch) == 0 {
		return nil
	}

	// Set a context with timeout for flushing messages.
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	// Use ProduceMessageWithObservability for each message in the batch
	for _, msg := range p.batch {
		err := obsKafka.ProduceMessageWithObservability(ctx, p.Writer, msg, p.observability.Tracer, p.observability.Logger)
		if err != nil {
			p.observability.Logger.Error("Failed to produce message with observability", zap.Error(err), zap.String("topic", p.Writer.Topic))
			return err
		}
	}

	p.observability.Logger.Info("Batch of messages flushed successfully", zap.String("topic", p.Writer.Topic), zap.Int("batchSize", len(p.batch)))
	p.batch = p.batch[:0] // Reset batch
	return nil
}

// Close closes the Kafka producer connection.
func (p *Producer) Close() error {
	// Flush any remaining messages before closing
	if err := p.Flush(context.Background()); err != nil {
		p.observability.Logger.Error("Failed to flush messages during close", zap.Error(err))
		return err
	}

	if err := p.Writer.Close(); err != nil {
		p.observability.Logger.Error("Failed to close Kafka producer", zap.Error(err))
		return err
	}

	p.observability.Logger.Info("Kafka producer closed successfully")
	return nil
}

// Map codec compression from configuration
func mapCompressionCodec(codec string) compress.Codec {
	switch codec {
	case "gzip":
		return compress.Gzip.Codec()
	case "snappy":
		return compress.Snappy.Codec()
	case "lz4":
		return compress.Lz4.Codec()
	case "zstd":
		return compress.Zstd.Codec()
	default:
		return nil // No compression by default
	}
}
