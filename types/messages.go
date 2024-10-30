// /messages/types/messages.go
package types

type MessageConfig struct {
	Kafka struct {
		Brokers     []string `mapstructure:"brokers"`
		Topic       string   `mapstructure:"topic"`
		GroupID     string   `mapstructure:"group_id"`
		BatchSize   int      `mapstructure:"batch_size"`
		Retries     int      `mapstructure:"retries"`
		Timeout     int      `mapstructure:"timeout"`
		Compression string   `mapstructure:"compression"`
		Offset      string   `mapstructure:"offset"`
	} `mapstructure:"kafka"`
}
