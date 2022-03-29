package cmd

import (
	"plugin"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/tahsinrahman/map-reduce/internal"
)

var (
	serverAddr       string
	outputFilePrefix string
	pluginPath       string
)

// workerCmd represents the worker command
var workerCmd = &cobra.Command{
	Use: "worker",
	// Args: cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		mapFunc, reduceFunc, err := loadPlugin(pluginPath)
		if err != nil {
			log.Err(err).Msg("failed to load plugins")
			return err
		}

		w := internal.NewWorker(internal.NewWorkerConfig{
			ServerAddress:    serverAddr,
			ReduceWorkers:    reduceWorkers,
			OutputFilePrefix: outputFilePrefix,
			MapFunc:          mapFunc,
			ReduceFunc:       reduceFunc,
		})

		return w.Run()
	},
}

func init() {
	rootCmd.AddCommand(workerCmd)

	workerCmd.Flags().StringVar(&serverAddr, "server-address", "127.0.0.1:8080", "coordinator server address")
	workerCmd.Flags().IntVar(&reduceWorkers, "reduce-tasks", 10, "total number of reduce tasks")
	workerCmd.Flags().StringVar(&pluginPath, "plugin-path", "", "path to plugin file")
	workerCmd.Flags().StringVar(&outputFilePrefix, "output-prefix", "", "prefix for output files")

	_ = workerCmd.MarkFlagRequired("plugin-path")
	_ = workerCmd.MarkFlagRequired("reduce-tasks")
	_ = workerCmd.MarkFlagRequired("output-prefix")
}

// load the application Map and Reduce functions
// from a plugin file
func loadPlugin(filename string) (internal.MapFunc, internal.ReduceFunc, error) {
	p, err := plugin.Open(filename)
	if err != nil {
		// log.Fatalf("cannot load plugin %v", filename)
		return nil, nil, err
	}

	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Err(err).Msg("failed to find Map")
		return nil, nil, err
	}
	mapFunc := xmapf.(func(string, string) []internal.KeyValue)

	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Err(err).Msg("failed to find Reduce")
		return nil, nil, err
	}
	reduceFunc := xreducef.(func(string, []string) string)

	return mapFunc, reduceFunc, nil
}
