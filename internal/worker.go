package internal

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	mapreducev1 "github.com/tahsinrahman/map-reduce/apis/mapreduce/v1"
)

type MapFunc func(string, string) []KeyValue
type ReduceFunc func(string, []string) string

type Worker struct {
	serverAddr       string
	reduceWorkers    int
	outputFilePrefix string
	mapFunc          MapFunc
	reduceFunc       ReduceFunc
	logger           zerolog.Logger
}

type NewWorkerConfig struct {
	ServerAddress    string
	ReduceWorkers    int
	OutputFilePrefix string
	MapFunc          MapFunc
	ReduceFunc       ReduceFunc
}

func NewWorker(conf NewWorkerConfig) *Worker {
	return &Worker{
		serverAddr:       conf.ServerAddress,
		reduceWorkers:    conf.ReduceWorkers,
		outputFilePrefix: conf.OutputFilePrefix,
		mapFunc:          conf.MapFunc,
		reduceFunc:       conf.ReduceFunc,
		logger:           zerolog.New(os.Stdout).With().Timestamp().Caller().Str("name", "worker").Logger(),
	}
}

func (w *Worker) Run() error {
	log := w.logger

	cc, err := grpc.Dial(w.serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	client := mapreducev1.NewMapReduceServiceClient(cc)

	g, ctx := errgroup.WithContext(context.Background())
	g.Go(func() error {
		return w.RunMapTasks(ctx, client)
	})
	g.Go(func() error {
		return w.RunReduceTasks(ctx, client)
	})

	if err := g.Wait(); err != nil {
		log.Err(err).Msg("failed waiting for map and reduce tasks")
		return err
	}

	return nil
}

func (w *Worker) RunMapTasks(ctx context.Context, client mapreducev1.MapReduceServiceClient) error {
	log := w.logger

	stream, err := client.AskForMapTask(ctx, &mapreducev1.AskForMapTaskRequest{})
	if err != nil {
		return err
	}
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
		}

		resp, err := stream.Recv()
		if err == io.EOF {
			log.Info().Msg("no more available map tasks")
			return nil
		}
		if err != nil {
			log.Err(err).Msg("failed to receive map task")
			return err
		}

		go func() {
			intermediateFiles, err := w.doMapTask(resp.Task, w.mapFunc)
			if err != nil {
				log.Err(err).Interface("task", resp.Task).Msg("failed performing map")
				return
			}

			_, err = client.FinishMapTask(ctx, &mapreducev1.FinishMapTaskRequest{
				TaskId:                     resp.Task.TaskId,
				TemporaryIntermediateFiles: intermediateFiles,
			})
			if err != nil {
				log.Err(err).Msg("failed to ask finish map task")
				return
			}
		}()
	}
}

func (w *Worker) RunReduceTasks(ctx context.Context, client mapreducev1.MapReduceServiceClient) error {
	log := w.logger

	stream, err := client.AskForReduceTask(ctx, &mapreducev1.AskForReduceTaskRequest{})
	if err != nil {
		return err
	}
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
		}

		resp, err := stream.Recv()
		if err == io.EOF {
			log.Info().Msg("no more available reduce tasks")
			return nil
		}
		if err != nil {
			log.Err(err).Msg("failed to receive reduce task")
			return err
		}

		go func() {
			if err := w.doReduceTask(resp.Task, w.reduceFunc); err != nil {
				log.Err(err).Msg("failed to perform reduce task")
				return
			}

			_, err = client.FinishReduceTask(ctx, &mapreducev1.FinishReduceTaskRequest{
				TaskId: resp.Task.TaskId,
			})
			if err != nil {
				log.Err(err).Msg("failed to ack finished map task")
				return
			}
		}()
	}
}

func (w *Worker) doMapTask(task *mapreducev1.MapTask, mapf MapFunc) ([]string, error) {
	log := w.logger
	log.Info().Interface("task", task).Msg("starting map task")

	contents, err := ioutil.ReadFile(task.InputFile)
	if err != nil {
		log.Err(err).Msg("failed to read file")
		return nil, err
	}

	intermediate := mapf(task.InputFile, string(contents))

	var files []*bufio.Writer
	var intermediateFiles []string

	for i := 0; i < w.reduceWorkers; i++ {
		f, err := ioutil.TempFile("", "map-intermediate")
		if err != nil {
			log.Err(err).Msg("failed to create reduce-files")
			return nil, err
		}
		files = append(files, bufio.NewWriter(f))
		intermediateFiles = append(intermediateFiles, f.Name())
	}

	for _, kv := range intermediate {
		index := ihash(kv.Key) % w.reduceWorkers
		_, err := files[index].WriteString(fmt.Sprintf("%s %s\n", kv.Key, kv.Value))
		if err != nil {
			log.Err(err).Msg("failed to write intermediate files")
			return nil, err
		}
	}
	for i := 0; i < w.reduceWorkers; i++ {
		if err := files[i].Flush(); err != nil {
			log.Err(err).Msg("failed to flush")
			return nil, err
		}
	}

	log.Info().Interface("task", task).Msg("successfully performed map task")
	return intermediateFiles, nil
}

func (w *Worker) doReduceTask(task *mapreducev1.ReduceTask, reduceFunc ReduceFunc) error {
	log := w.logger.With().Int32("task", task.TaskId).Logger()
	log.Info().Msg("starting reduce task")

	var intermediate []KeyValue
	for _, f := range task.IntermediateFiles {
		file, err := os.Open(f)
		if err != nil {
			log.Err(err).Str("file", f).Msg("failed to open intermediate file")
			return err
		}
		var key string
		var value string
		for {
			_, err := fmt.Fscanf(file, "%s %s", &key, &value)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Err(err).Str("file", f).Msg("failed to scan file")
				return err
			}
			intermediate = append(intermediate, KeyValue{
				Key:   key,
				Value: value,
			})
		}
	}
	sort.Sort(ByKey(intermediate))

	outputFileName := fmt.Sprintf("%s%d", w.outputFilePrefix, task.TaskId)
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		log.Err(err).Msg("failed to create output file")
		return err
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reduceFunc(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err = fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			log.Err(err).Msg("failed to write to output file")
		}

		i = j
	}

	log.Info().Msg("successfully performed reduce task")
	return nil
}
