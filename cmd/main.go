package main

import (
	"context"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stream-stack/publisher/pkg/config"
	"github.com/stream-stack/publisher/pkg/store"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"time"
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
			logrus.SetLevel(logrus.TraceLevel)
			rand.Seed(time.Now().UnixNano())
			logrus.Debug("[config]env print:")
			for _, s := range os.Environ() {
				if strings.HasPrefix(s, "STREAM_DISPATCHER") {
					logrus.Debug(s)
				}
			}

			logrus.Debugf("[config]dump config:%v", viper.AllSettings())
			if err := store.Start(ctx); err != nil {
				return err
			}

			<-ctx.Done()
			return nil
		},
	}
	store.InitFlags()

	viper.AddConfigPath(`./config`)
	viper.SetConfigName("config")
	config.BuildFlags(command)
	viper.SetEnvPrefix("stream_publisher")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		logrus.Errorf("[config]read config error:%v", err)
	}
	if err := viper.BindPFlags(command.PersistentFlags()); err != nil {
		logrus.Errorf("[config]BindPFlags config error:%v", err)
	}

	return command, ctx, cancelFunc
}

func main() {
	command, _, _ := NewCommand()
	if err := command.Execute(); err != nil {
		panic(err)
	}
}
