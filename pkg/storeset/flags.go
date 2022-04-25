package storeset

import (
	"github.com/spf13/cobra"
	"github.com/stream-stack/publisher/pkg/config"
	"time"
)

var managerAddr string
var connectionRetryDuration time.Duration

var selector string
var streamName string
var broker string

func InitFlags() {
	config.RegisterFlags(func(command *cobra.Command) {
		command.PersistentFlags().StringVar(&managerAddr, "ManagerAddr", "0.0.0.0:8080", "manager grpc address")
		command.PersistentFlags().DurationVar(&connectionRetryDuration, "ConnectionRetryDuration", time.Second, "connection retry duration")
		command.PersistentFlags().StringVar(&broker, "BROKER", "", "broker name")

		command.PersistentFlags().StringVar(&selector, "SELECTOR", "", "storeset selector")
		command.PersistentFlags().StringVar(&streamName, "STREAM_NAME", "", "stream name")
	})
}
