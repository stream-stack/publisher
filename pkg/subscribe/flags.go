package subscribe

import (
	"github.com/spf13/cobra"
	"github.com/stream-stack/store/pkg/config"
	"time"
)

var ackDuration time.Duration
var recvDuration time.Duration
var senderMaxRequestDuration time.Duration
var senderMaxRequestRetryCount int

func InitFlags() {
	config.RegisterFlags(func(command *cobra.Command) {
		command.PersistentFlags().DurationVar(&ackDuration, "AckDuration", time.Second*5, "ack duration")
		command.PersistentFlags().DurationVar(&recvDuration, "recvDuration", time.Second, "recv duration")
		command.PersistentFlags().DurationVar(&senderMaxRequestDuration, "sender-MaxRequestDuration", time.Second*10, "MaxRequestDuration")
		command.PersistentFlags().IntVar(&senderMaxRequestRetryCount, "sender-MaxRequestRetryCount", 10, "MaxRequestRetryCount")
	})
}
