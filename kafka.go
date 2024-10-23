package messages

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

// KafkaProducer manages producing messages to Kafka.
type KafkaProducer struct {
	Writer *kafka.Writer
}

// NewKafkaProducer creates a new Kafka producer.
func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	return &KafkaProducer{
		Writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers: brokers,
			Topic:   topic,
		}),
	}
}

// SendMessage sends a message to the Kafka topic.
func (p *KafkaProducer) SendMessage(ctx context.Context, key, value string) error {
	err := p.Writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: []byte(value),
	})
	if err != nil {
		log.Printf("Failed to send message: %v", err)
		return err
	}
	log.Printf("Message sent: %s", value)
	return nil
}

// Close closes the Kafka producer connection.
func (p *KafkaProducer) Close() error {
	return p.Writer.Close()
}

// KafkaConsumer manages consuming messages from Kafka.
type KafkaConsumer struct {
	Reader *kafka.Reader
}

// NewKafkaConsumer creates a new Kafka consumer.
func NewKafkaConsumer(brokers []string, topic, groupID string) *KafkaConsumer {
	return &KafkaConsumer{
		Reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
	}
}

// ReadMessage reads a message from the Kafka topic.
func (c *KafkaConsumer) ReadMessage(ctx context.Context) (string, string, error) {
	msg, err := c.Reader.ReadMessage(ctx)
	if err != nil {
		log.Printf("Failed to read message: %v", err)
		return "", "", err
	}
	log.Printf("Message received: %s", msg.Value)
	return string(msg.Key), string(msg.Value), nil
}

// Close closes the Kafka consumer connection.
func (c *KafkaConsumer) Close() error {
	return c.Reader.Close()
}
