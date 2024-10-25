// /messages/main.go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/goletan/config"
	"github.com/goletan/messages/kafka"
	"github.com/goletan/messages/types"
	"go.uber.org/zap"
)

var cfg types.MessageConfig

func main() {
	// Initialize logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// Load Kafka configuration

	err := config.LoadConfig("Messages", &cfg, nil)
	if err != nil {
		log.Fatalf("Failed to load Kafka config: %v", err)
	}

	// Create context with cancel for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle system interrupts for graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalChan
		logger.Info("Received shutdown signal")
		cancel()
	}()

	// Create Kafka consumer and producer
	consumer := kafka.NewKafkaConsumer(&cfg, logger)
	defer consumer.Close()

	producer := kafka.NewKafkaProducer(&cfg, logger)
	defer producer.Close()

	// Process messages from consumer
	go func() {
		for {
			select {
			case <-ctx.Done():
				logger.Info("Stopping message processing")
				return
			default:
				key, value, err := consumer.ReadMessage(ctx)
				if err != nil {
					logger.Error("Error reading message", zap.Error(err))
					continue
				}

				err = handleMessage(ctx, producer, key, value)
				if err != nil {
					logger.Error("Error handling message", zap.Error(err))
				}
			}
		}
	}()

	// Wait for context to be canceled
	<-ctx.Done()
	logger.Info("Application stopped gracefully")
}

// handleMessage processes a message and sends a response via producer.
func handleMessage(ctx context.Context, producer *kafka.KafkaProducer, key, value string) error {
	// Implement message processing logic here
	// Example: send an acknowledgment back to Kafka
	ackKey := key + ":ack"
	ackValue := "Processed: " + value
	return producer.SendMessage(ctx, ackKey, ackValue)
}
