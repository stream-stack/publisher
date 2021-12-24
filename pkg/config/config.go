package config

import "github.com/spf13/cobra"

type AddFlags func(command *cobra.Command)

var flagSlice = make([]AddFlags, 0)

func RegisterFlags(flags AddFlags) {
	flagSlice = append(flagSlice, flags)
}

func BuildFlags(command *cobra.Command) {
	for _, f := range flagSlice {
		f(command)
	}
}
