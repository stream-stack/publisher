package store

import (
	"github.com/spf13/cobra"
	"github.com/stream-stack/publisher/pkg/config"
	"time"
)

func InitFlags() {
	config.RegisterFlags(func(c *cobra.Command) {
		c.PersistentFlags().Duration("store-timeout", time.Second*2, "store cloudevent timeout")
		c.PersistentFlags().String("store-partition-config-file", "./conf/partitions.json", "store partition config file")
		c.PersistentFlags().Duration("virtual-actor-lifetime", time.Second*10, "virtual actor lifetime")
		c.PersistentFlags().String("subscribe-url", "http://www.github.com", "subscribe url")
		c.PersistentFlags().Duration("offset-sync-interval", time.Minute, "offset sync interval")
	})
}
