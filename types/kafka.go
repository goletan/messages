package types

type MessageConfig struct {
	Kafka struct {
		Brokers   []string `mapstructure:"brokers"`
		Topic     string   `mapstructure:"topic"`
		GroupID   string   `mapstructure:"group_id"`
		BatchSize int      `mapstructure:"batch_size"`
	} `mapstructure:"messages"`
}
