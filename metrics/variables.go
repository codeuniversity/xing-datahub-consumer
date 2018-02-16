package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

//MessagesConsumed counts the consumed Kafka Messages
var MessagesConsumed = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "messages_consumed",
		Help: "Kafka messages consumed partitioned by record type and success",
	},
	[]string{"record", "success"},
)

//BatchesExported counts the consumed Kafka Messages
var BatchesExported = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "batches_exported",
		Help: "Batches exported partitioned by record type",
	},
	[]string{"record"},
)
