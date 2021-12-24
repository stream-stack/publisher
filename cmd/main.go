package main

import (
	"context"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stream-stack/publisher/pkg/config"
	"github.com/stream-stack/publisher/pkg/subscribe"
	"os"
	"os/signal"
)

func NewCommand() (*cobra.Command, context.Context, context.CancelFunc) {
	ctx, cancelFunc := context.WithCancel(context.TODO())
	command := &cobra.Command{
		Use:   ``,
		Short: ``,
		Long:  ``,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			go func() {
				c := make(chan os.Signal, 1)
				signal.Notify(c, os.Kill)
				<-c
				cancelFunc()
			}()
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := subscribe.StartManager(ctx); err != nil {
				return err
			}

			<-ctx.Done()
			return nil
		},
	}
	subscribe.InitFlags()
	config.InitFlags()

	viper.AutomaticEnv()
	viper.AddConfigPath(`.`)
	config.BuildFlags(command)

	return command, ctx, cancelFunc
}

func main() {
	command, _, _ := NewCommand()
	if err := command.Execute(); err != nil {
		panic(err)
	}
}
