module github.com/goletan/messages

go 1.23.2

require (
	github.com/goletan/observability v0.0.0-00010101000000-000000000000
	github.com/prometheus/client_golang v1.20.5
	github.com/segmentio/kafka-go v0.4.47
	go.uber.org/zap v1.27.0
)

replace github.com/goletan/config => ../config

replace github.com/goletan/observability => ../observability

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/sys v0.25.0 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)
