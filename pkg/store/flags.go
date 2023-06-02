package store

import (
	"github.com/spf13/cobra"
	"github.com/stream-stack/publisher/pkg/config"
	"time"
)

func InitFlags() {
	config.RegisterFlags(func(c *cobra.Command) {
		c.PersistentFlags().Duration("StoreTimeout", time.Second*2, "store cloudevent timeout")
		c.PersistentFlags().String("StorePartitionConfigFile", "./conf/partitions.json", "store partition config file")
		c.PersistentFlags().Duration("VirtualActorLifetime", time.Second*10, "virtual actor lifetime")
		c.PersistentFlags().String("SubscribeUrl", "http://www.github.com", "subscribe url")
		c.PersistentFlags().Duration("OffsetSyncInterval", time.Minute, "offset sync interval")
	})
}
