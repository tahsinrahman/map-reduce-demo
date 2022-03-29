package internal

import (
	"context"
	"net"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	mapreducev1 "github.com/tahsinrahman/map-reduce/apis/mapreduce/v1"
)

type NewCoordinatorConfig struct {
	ListenAddress string
	InputFiles    []string
	ReduceWorkers int
	Timeout       int
}

func NewCoordinator(config NewCoordinatorConfig) *Coordinator {
	var mapTasks []*mapreducev1.MapTask
	for i, f := range config.InputFiles {
		mapTasks = append(mapTasks, &mapreducev1.MapTask{
			TaskId:    int32(i),
			InputFile: f,
			State:     mapreducev1.State_STATE_IDLE_UNSPECIFIED,
		})
	}

	var reduceTasks []*mapreducev1.ReduceTask
	for i := 0; i < config.ReduceWorkers; i++ {
		reduceTasks = append(reduceTasks, &mapreducev1.ReduceTask{
			TaskId:            int32(i),
			IntermediateFiles: nil,
			State:             mapreducev1.State_STATE_IDLE_UNSPECIFIED,
		})
	}

	mapTasksCh := make(chan *mapreducev1.MapTask, len(mapTasks))
	for _, t := range mapTasks {
		mapTasksCh <- t
	}
	reduceTasksCh := make(chan *mapreducev1.ReduceTask)

	return &Coordinator{
		listenAddr:           config.ListenAddress,
		mapTasks:             mapTasks,
		reduceTasks:          reduceTasks,
		availableMapTasks:    mapTasksCh,
		availableReduceTasks: reduceTasksCh,
		reduceWorkerCount:    config.ReduceWorkers,
		logger:               zerolog.New(os.Stdout).With().Caller().Timestamp().Str("name", "coordinator").Logger(),

		timeout:               time.Duration(config.Timeout) * time.Second,
		inProgressMapTasks:    make(chan int32, len(mapTasks)),
		inProgressReduceTasks: make(chan int32, len(reduceTasks)),
	}
}

type Coordinator struct {
	mu                   sync.RWMutex
	listenAddr           string
	mapTasks             []*mapreducev1.MapTask
	reduceTasks          []*mapreducev1.ReduceTask
	availableMapTasks    chan *mapreducev1.MapTask
	availableReduceTasks chan *mapreducev1.ReduceTask
	reduceWorkerCount    int
	logger               zerolog.Logger

	timeout               time.Duration
	inProgressMapTasks    chan int32
	inProgressReduceTasks chan int32
}

func (c *Coordinator) Run() error {
	log := c.logger

	listener, err := net.Listen("tcp", c.listenAddr)
	if err != nil {
		log.Err(err).Msg("failed to create listener")
		return err
	}

	srv := grpc.NewServer()
	mapreducev1.RegisterMapReduceServiceServer(srv, c)

	g := errgroup.Group{}
	g.Go(func() error {
		// start the grpc server
		log.Info().Str("addr", c.listenAddr).Msg("starting server")
		if err := srv.Serve(listener); err != nil {
			srv.GracefulStop()
			return err
		}
		return nil
	})
	g.Go(func() error {
		c.ReAssignMap()
		return nil
	})
	g.Go(func() error {
		c.ReAssignReduce()
		return nil
	})
	g.Go(func() error {
		c.Check()
		srv.GracefulStop()
		return nil
	})

	if err := g.Wait(); err != nil {
		log.Err(err).Msg("failed coordinator")
		return err
	}

	return nil
}

func (c *Coordinator) Check() {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			if c.Checker() {
				return
			}
		}
	}
}

func (c *Coordinator) Checker() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, t := range c.mapTasks {
		if t.State != mapreducev1.State_STATE_COMPLETED {
			return false
		}
	}
	for _, t := range c.reduceTasks {
		if t.State != mapreducev1.State_STATE_COMPLETED {
			return false
		}
	}

	return true
}

func (c *Coordinator) AskForMapTask(_ *mapreducev1.AskForMapTaskRequest, stream mapreducev1.MapReduceService_AskForMapTaskServer) error {
	log := c.logger

	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
		}
		task, ok := <-c.availableMapTasks
		if !ok {
			// channel closed
			log.Info().Msg("no more map task to assign")
			return nil
		}
		log := log.With().Int32("task", task.TaskId).Logger()

		c.inProgressMapTasks <- task.TaskId

		log.Info().Msg("assigning map task")
		err := stream.Send(&mapreducev1.AskForMapTaskResponse{
			Task: task,
		})

		if err != nil {
			log.Err(err).Msg("failed to send map task to worker")
			return err
		}
	}
}

func (c *Coordinator) AskForReduceTask(_ *mapreducev1.AskForReduceTaskRequest, stream mapreducev1.MapReduceService_AskForReduceTaskServer) error {
	log := c.logger
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		default:
		}
		task, ok := <-c.availableReduceTasks
		if !ok {
			// channel closed
			log.Info().Msg("no more reduce task to assign")
			return nil
		}
		c.inProgressReduceTasks <- task.TaskId

		log.Info().Int32("task", task.TaskId).Msg("assigning reduce task")
		err := stream.Send(&mapreducev1.AskForReduceTaskResponse{
			Task: task,
		})
		if err != nil {
			log.Err(err).Msg("failed to send reduce task to worker")
			return err
		}
	}
}

func (c *Coordinator) FinishMapTask(_ context.Context, req *mapreducev1.FinishMapTaskRequest) (*mapreducev1.FinishMapTaskResponse, error) {
	log := c.logger.With().Int32("task", req.TaskId).Logger()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mapTasks[req.TaskId].State == mapreducev1.State_STATE_COMPLETED {
		log.Info().Msg("map task already finished")
		return &mapreducev1.FinishMapTaskResponse{}, nil
	}

	log.Info().Int32("task", req.TaskId).Msg("finished map task")

	c.mapTasks[req.TaskId].State = mapreducev1.State_STATE_COMPLETED
	for i, f := range req.TemporaryIntermediateFiles {
		c.reduceTasks[i].IntermediateFiles = append(c.reduceTasks[i].IntermediateFiles, f)
		if len(c.reduceTasks[i].IntermediateFiles) == len(c.mapTasks) {
			c.availableReduceTasks <- c.reduceTasks[i]
		}
	}

	allCompleted := true
	for _, t := range c.mapTasks {
		if t.State != mapreducev1.State_STATE_COMPLETED {
			allCompleted = false
			break
		}
	}
	if allCompleted {
		close(c.availableMapTasks)
		close(c.inProgressMapTasks)
	}

	return &mapreducev1.FinishMapTaskResponse{}, nil
}

func (c *Coordinator) FinishReduceTask(_ context.Context, req *mapreducev1.FinishReduceTaskRequest) (*mapreducev1.FinishReduceTaskResponse, error) {
	log := c.logger.With().Int32("task", req.TaskId).Logger()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.reduceTasks[req.TaskId].State == mapreducev1.State_STATE_COMPLETED {
		log.Info().Msg("reduce task already finished")
		return &mapreducev1.FinishReduceTaskResponse{}, nil
	}

	log.Info().Msg("finished reduce task")
	c.reduceTasks[req.TaskId].State = mapreducev1.State_STATE_COMPLETED

	allCompleted := true
	for _, t := range c.reduceTasks {
		if t.State != mapreducev1.State_STATE_COMPLETED {
			allCompleted = false
			break
		}
	}
	if allCompleted {
		close(c.availableReduceTasks)
		close(c.inProgressReduceTasks)
	}

	return &mapreducev1.FinishReduceTaskResponse{}, nil
}

func (c *Coordinator) ReAssignMap() {
	log := c.logger

	wg := sync.WaitGroup{}
	for task := range c.inProgressMapTasks {
		wg.Add(1)
		go func(task int32) {
			defer wg.Done()

			timeout := time.After(c.timeout)
			ticker := time.NewTicker(250 * time.Millisecond)

			for {
				select {
				case <-ticker.C:
					c.mu.RLock()
					state := c.mapTasks[task].State
					c.mu.RUnlock()
					if state == mapreducev1.State_STATE_COMPLETED {
						return
					}
				case <-timeout:
					c.mu.Lock()
					c.mapTasks[task].State = mapreducev1.State_STATE_IDLE_UNSPECIFIED
					c.mu.Unlock()

					log.Info().Int32("task", task).Msg("timeout map task, re-queueing task")

					c.availableMapTasks <- c.mapTasks[task]
					return
				}
			}
		}(task)
	}
	wg.Wait()
}

func (c *Coordinator) ReAssignReduce() {
	log := c.logger

	wg := sync.WaitGroup{}
	for task := range c.inProgressReduceTasks {
		wg.Add(1)
		go func(task int32) {
			defer wg.Done()

			timeout := time.After(c.timeout)
			ticker := time.NewTicker(250 * time.Millisecond)

			for {
				select {
				case <-ticker.C:
					c.mu.RLock()
					state := c.reduceTasks[task].State
					c.mu.RUnlock()
					if state == mapreducev1.State_STATE_COMPLETED {
						return
					}
				case <-timeout:
					c.mu.Lock()
					c.reduceTasks[task].State = mapreducev1.State_STATE_IDLE_UNSPECIFIED
					c.mu.Unlock()

					log.Info().Int32("task", task).Msg("timeout reduce task, re-queueing task")

					c.availableReduceTasks <- c.reduceTasks[task]
					return
				}
			}
		}(task)
	}
	wg.Wait()
}
