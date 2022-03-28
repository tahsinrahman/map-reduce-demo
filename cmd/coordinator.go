package cmd

import (
	"github.com/spf13/cobra"
)

// coordinatorCmd represents the coOrdinator command
var coordinatorCmd = &cobra.Command{
	Use: "coordinator",
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func init() {
	rootCmd.AddCommand(coordinatorCmd)
}
