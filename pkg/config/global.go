package config

import "github.com/spf13/cobra"

var Address string

func InitFlags() {
	RegisterFlags(func(command *cobra.Command) {
		command.PersistentFlags().StringVar(&Address, "Address", "localhost:2001,localhost:2002,localhost:2003", "TCP host+port for this node")
	})
}
