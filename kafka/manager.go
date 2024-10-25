// /messages/kafka/manager.go
package kafka

import (
	"log"

	"github.com/goletan/config"
	"github.com/goletan/messages/types"
)

var cfg types.MessageConfig

// LoadConfig loads the shared Kafka configuration.
func LoadConfig() *types.MessageConfig {
	err := config.LoadConfig("Messages", &cfg, nil)
	if err != nil {
		log.Fatalf("Failed to load Kafka config: %v", err)
	}

	return &cfg
}
