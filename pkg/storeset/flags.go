package storeset

import (
	"github.com/spf13/cobra"
	"github.com/stream-stack/publisher/pkg/config"
	"time"
)

var managerAddr string
var ackDuration time.Duration
var connectionRetryDuration time.Duration

var senderMaxRequestDuration time.Duration
var senderMaxRequestRetryCount int

func InitFlags() {
	config.RegisterFlags(func(command *cobra.Command) {
		command.PersistentFlags().DurationVar(&ackDuration, "AckDuration", time.Second*5, "ack duration")
		command.PersistentFlags().StringVar(&managerAddr, "ManagerAddr", "0.0.0.0:8080", "manager grpc address")
		command.PersistentFlags().DurationVar(&connectionRetryDuration, "ConnectionRetryDuration", time.Second, "connection retry duration")

		command.PersistentFlags().DurationVar(&senderMaxRequestDuration, "sender-MaxRequestDuration", time.Second*10, "MaxRequestDuration")
		command.PersistentFlags().IntVar(&senderMaxRequestRetryCount, "sender-MaxRequestRetryCount", 10, "MaxRequestRetryCount")
	})
}
