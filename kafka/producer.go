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
