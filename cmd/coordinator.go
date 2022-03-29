package cmd

import (
	"errors"

	"github.com/spf13/cobra"

	"github.com/tahsinrahman/map-reduce/internal"
)

var (
	reduceWorkers int
	listenAddr    string
	timeout       int
)

// coordinatorCmd represents the coOrdinator command
var coordinatorCmd = &cobra.Command{
	Use: "coordinator",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return errors.New("expected at least one input file")
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		inputFiles := args
		c := internal.NewCoordinator(internal.NewCoordinatorConfig{
			ListenAddress: listenAddr,
			InputFiles:    inputFiles,
			ReduceWorkers: reduceWorkers,
			Timeout:       timeout,
		})
		return c.Run()
	},
}

func init() {
	rootCmd.AddCommand(coordinatorCmd)

	coordinatorCmd.Flags().IntVar(&reduceWorkers, "reduce-tasks", 10, "total reduce tasks")
	coordinatorCmd.Flags().StringVar(&listenAddr, "listen-addr", ":8080", "coordinator listen address")
	coordinatorCmd.Flags().IntVar(&timeout, "timeout", 10, "timeout in seconds")

	_ = coordinatorCmd.MarkFlagRequired("reduce-tasks")
}
