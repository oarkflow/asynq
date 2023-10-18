// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/oarkflow/xid"

	"github.com/oarkflow/asynq/base"
	"github.com/oarkflow/asynq/log"
	"github.com/oarkflow/asynq/rdb"

	"github.com/redis/go-redis/v9"
)

// Server is responsible for task processing and task lifecycle management.
//
// Server pulls tasks off queues and processes them.
// If the processing of a task is unsuccessful, server will schedule it for a retry.
//
// A task will be retried until either the task gets processed successfully
// or until it reaches its max retry count.
//
// If a task exhausts its retries, it will be moved to the archive and
// will be kept in the archive set.
// Note that the archive size is finite and once it reaches its max size,
// the oldest tasks in the archive will be deleted.
type Server struct {
	ServerID    string
	queues      map[string]int
	handler     *ServeMux
	concurrency int
	logger      *log.Logger
	broker      base.Broker
	// When a Server has been created with an existing Redis connection, we do
	// not want to close it.
	sharedConnection bool
	state            *serverState
	strictPriority   bool
	// wait group to wait for all goroutines to finish.
	wg            sync.WaitGroup
	forwarder     *forwarder
	processor     *processor
	syncer        *syncer
	heartbeater   *heartbeater
	subscriber    *subscriber
	recoverer     *recoverer
	healthchecker *healthchecker
	janitor       *janitor
	aggregator    *aggregator
}

type serverState struct {
	mu    sync.Mutex
	value serverStateValue
}

type serverStateValue int

const (
	// StateNew represents a new server. Server begins in
	// this state and then transition to StatusActive when
	// Start or Run is callled.
	srvStateNew serverStateValue = iota

	// StateActive indicates the server is up and active.
	srvStateActive

	// StateStopped indicates the server is up but no longer processing new tasks.
	srvStateStopped

	// StateClosed indicates the server has been shutdown.
	srvStateClosed
)

var serverStates = []string{
	"new",
	"active",
	"stopped",
	"closed",
}

func (s serverStateValue) String() string {
	if srvStateNew <= s && s <= srvStateClosed {
		return serverStates[s]
	}
	return "unknown status"
}

// Config specifies the server's background-task processing behavior.
type Config struct {
	RedisClientOpt RedisClientOpt `json:"redis_opt"`
	Mode           Mode           `json:"mode"`
	NoService      bool           `json:"no_service"`
	FlowID         string         `json:"flow_id"`
	FlowPrefix     string         `json:"flow_prefix"`
	UserID         any            `json:"user_id"`
	Concurrency    int            `json:"concurrency"`
	RedisServer    string         `json:"redis_server"`
	RDB            *rdb.RDB
	// Maximum number of concurrent processing of tasks.
	//
	// If set to a zero or negative value, NewServer will overwrite the value
	// to the number of CPUs usable by the current process.

	// BaseContext optionally specifies a function that returns the base context for Handler invocations on this server.
	//
	// If BaseContext is nil, the default is context.Background().
	// If this is defined, then it MUST return a non-nil context
	BaseContext func() context.Context

	// TaskCheckInterval specifies the interval between checks for new tasks to process when all queues are empty.
	//
	// Be careful not to set this value too low because it adds significant load to redis.
	//
	// If set to a zero or negative value, NewServer will overwrite the value with default value.
	//
	// By default, TaskCheckInterval is set to 1 seconds.
	TaskCheckInterval time.Duration
	// Function to calculate retry delay for a failed task.
	//
	// By default, it uses exponential backoff algorithm to calculate the delay.
	RetryDelayFunc RetryDelayFunc

	// Predicate function to determine whether the error returned from Handler is a failure.
	// If the function returns false, Server will not increment the retried counter for the task,
	// and Server won't record the queue stats (processed and failed stats) to avoid skewing the error
	// rate of the queue.
	//
	// By default, if the given error is non-nil the function returns true.
	IsFailure func(error) bool

	// List of queues to process with given priority value. Keys are the names of the
	// queues and values are associated priority value.
	//
	// If set to nil or not specified, the server will process only the "default" queue.
	//
	// Priority is treated as follows to avoid starving low priority queues.
	//
	// Example:
	//
	//     Queues: map[string]int{
	//         "critical": 6,
	//         "default":  3,
	//         "low":      1,
	//     }
	//
	// With the above Config and given that all queues are not empty, the tasks
	// in "critical", "default", "low" should be processed 60%, 30%, 10% of
	// the time respectively.
	//
	// If a queue has a zero or negative priority value, the queue will be ignored.
	Queues map[string]int

	// StrictPriority indicates whether the queue priority should be treated strictly.
	//
	// If set to true, tasks in the queue with the highest priority is processed first.
	// The tasks in lower priority queues are processed only when those queues with
	// higher priorities are empty.
	StrictPriority bool
	// RecoverPanicFunc will be inject some actions when workers panic.

	// Example:

	//     func pushPanicErrorToSentry(errMsg string) {
	//      // perform to push error message to Sentry.
	//     })
	RecoverPanicFunc RecoverPanicFunc

	// ErrorHandler handles errors returned by the task handler.
	//
	// HandleError is invoked only if the task handler returns a non-nil error.
	//
	// Example:
	//
	//     func reportError(ctx context, task *asynq.Task, err error) {
	//         retried, _ := asynq.GetRetryCount(ctx)
	//         maxRetry, _ := asynq.GetMaxRetry(ctx)
	//     	   if retried >= maxRetry {
	//             err = fmt.Errorf("retry exhausted for task %s: %w", task.Type, err)
	//     	   }
	//         errorReportingService.Notify(err)
	//     })
	//
	//     ErrorHandler: asynq.ErrorHandlerFunc(reportError)
	ErrorHandler ErrorHandler

	CompleteHandler CompleteHandler

	DoneHandler DoneHandler

	CronReportHandler Handler

	// Logger specifies the logger used by the server instance.
	//
	// If unset, default logger is used.
	Logger Logger

	// LogLevel specifies the minimum log level to enable.
	//
	// If unset, InfoLevel is used by default.
	LogLevel LogLevel

	// ShutdownTimeout specifies the duration to wait to let workers finish their tasks
	// before forcing them to abort when stopping the server.
	//
	// If unset or zero, default timeout of 8 seconds is used.
	ShutdownTimeout time.Duration

	// HealthCheckFunc is called periodically with any errors encountered during ping to the
	// connected redis server.
	HealthCheckFunc func(error)

	// HealthCheckInterval specifies the interval between healthchecks.
	//
	// If unset or zero, the interval is set to 15 seconds.
	HealthCheckInterval time.Duration

	// DelayedTaskCheckInterval specifies the interval between checks run on 'scheduled' and 'retry'
	// tasks, and forwarding them to 'pending' state if they are ready to be processed.
	//
	// If unset or zero, the interval is set to 5 seconds.
	DelayedTaskCheckInterval time.Duration

	ServerID string

	// GroupGracePeriod specifies the amount of time the server will wait for an incoming task before aggregating
	// the tasks in a group. If an incoming task is received within this period, the server will wait for another
	// period of the same length, up to GroupMaxDelay if specified.
	//
	// If unset or zero, the grace period is set to 1 minute.
	// Minimum duration for GroupGracePeriod is 1 second. If value specified is less than a second, the call to
	// NewServer will panic.
	GroupGracePeriod time.Duration

	// GroupMaxDelay specifies the maximum amount of time the server will wait for incoming tasks before aggregating
	// the tasks in a group.
	//
	// If unset or zero, no delay limit is used.
	GroupMaxDelay time.Duration

	// GroupMaxSize specifies the maximum number of tasks that can be aggregated into a single task within a group.
	// If GroupMaxSize is reached, the server will aggregate the tasks into one immediately.
	//
	// If unset or zero, no size limit is used.
	GroupMaxSize int

	// GroupAggregator specifies the aggregation function used to aggregate multiple tasks in a group into one task.
	//
	// If unset or nil, the group aggregation feature will be disabled on the server.
	GroupAggregator GroupAggregator

	// StateChanged called when a task state changed
	//
	TaskStateProber *TaskStateProber

	// JanitorInterval specifies the average interval of janitor checks for expired completed tasks.
	//
	// If unset or zero, default interval of 8 seconds is used.
	JanitorInterval time.Duration

	// JanitorBatchSize specifies the number of expired completed tasks to be deleted in one run.
	//
	// If unset or zero, default batch size of 100 is used.
	// Make sure to not put a big number as the batch size to prevent a long-running script.
	JanitorBatchSize int

	idKey        string
	statusKey    string
	operationKey string
	flowIDKey    string
}

// TaskStateProber tell there's a state changed happening
type TaskStateProber struct {
	Probers map[string]string // map[state-string]data-name
	Handler func(map[string]interface{})
}

func (p TaskStateProber) Changed(out map[string]interface{}) {
	if p.Handler != nil {
		p.Handler(out)
	}
}

func (p TaskStateProber) Result(state base.TaskState, raw *base.TaskInfo) (key string, data interface{}) {
	defer func() {
		if len(key) == 0 {
			key = "task"
		}
		if data == nil {
			data = *newTaskInfo(raw.Message, raw.State, raw.NextProcessAt, raw.Result)
		}
	}()

	probers := p.Probers
	if len(probers) == 0 {
		probers = map[string]string{"*": "task"}
	}
	key, ok := probers["*"]
	if !ok {
		key, ok = probers[state.String()]
	}
	if !ok {
		return
	}

	switch key {
	case "next":
		data = raw.NextProcessAt
	case "result":
		if len(raw.Result) > 0 {
			data = raw.Result
		}
	}
	return
}

// GroupAggregator aggregates a group of tasks into one before the tasks are passed to the Handler.
type GroupAggregator interface {
	// Aggregate aggregates the given tasks in a group with the given group name,
	// and returns a new task which is the aggregation of those tasks.
	//
	// Use NewTask(typename, payload, opts...) to set any options for the aggregated task.
	// The Queue option, if provided, will be ignored and the aggregated task will always be enqueued
	// to the same queue the group belonged.
	Aggregate(group string, tasks []*Task) *Task
}

// The GroupAggregatorFunc type is an adapter to allow the use of  ordinary functions as a GroupAggregator.
// If f is a function with the appropriate signature, GroupAggregatorFunc(f) is a GroupAggregator that calls f.
type GroupAggregatorFunc func(group string, tasks []*Task) *Task

// Aggregate calls fn(group, tasks)
func (fn GroupAggregatorFunc) Aggregate(group string, tasks []*Task) *Task {
	return fn(group, tasks)
}

// An ErrorHandler handles an error occurred during task processing.
type ErrorHandler interface {
	HandleError(ctx context.Context, task *Task, err error)
}

// The ErrorHandlerFunc type is an adapter to allow the use of  ordinary functions as a ErrorHandler.
// If f is a function with the appropriate signature, ErrorHandlerFunc(f) is a ErrorHandler that calls f.
type ErrorHandlerFunc func(ctx context.Context, task *Task, err error)

// HandleError calls fn(ctx, task, err)
func (fn ErrorHandlerFunc) HandleError(ctx context.Context, task *Task, err error) {
	fn(ctx, task, err)
}

// An CompleteHandler handles an error occurred during task processing.
type CompleteHandler interface {
	HandleComplete(ctx context.Context, task *Task)
}

// The CompleteHandlerFunc type is an adapter to allow the use of  ordinary functions as a CompleteHandler.
// If f is a function with the appropriate signature, CompleteHandlerFunc(f) is a CompleteHandler that calls f.
type CompleteHandlerFunc func(ctx context.Context, task *Task)

// HandleComplete calls fn(ctx, task, err)
func (fn CompleteHandlerFunc) HandleComplete(ctx context.Context, task *Task) {
	fn(ctx, task)
}

// An DoneHandler handles an error occurred during task processing.
type DoneHandler interface {
	HandleDone(ctx context.Context, task *Task)
}

// The DoneHandlerFunc type is an adapter to allow the use of  ordinary functions as a DoneHandler.
// If f is a function with the appropriate signature, DoneHandlerFunc(f) is a DoneHandler that calls f.
type DoneHandlerFunc func(ctx context.Context, task *Task)

// HandleDone calls fn(ctx, task, err)
func (fn DoneHandlerFunc) HandleDone(ctx context.Context, task *Task) {
	fn(ctx, task)
}

// RecoverPanicFunc is used to inject some actions which will be performed when workers catch panic.
type RecoverPanicFunc func(errMsg string)

// RetryDelayFunc calculates the retry delay duration for a failed task given
// the retry count, error, and the task.
//
// n is the number of times the task has been retried.
// e is the error returned by the task handler.
// t is the task in question.
type RetryDelayFunc func(n int, e error, t *Task) time.Duration

// Logger supports logging at various log levels.
type Logger interface {
	// Debug logs a message at Debug level.
	Debug(args ...any)

	// Info logs a message at Info level.
	Info(args ...any)

	// Warn logs a message at Warning level.
	Warn(args ...any)

	// Error logs a message at Error level.
	Error(args ...any)

	// Fatal logs a message at Fatal level
	// and process will exit with status set to 1.
	Fatal(args ...any)
}

// LogLevel represents logging level.
//
// It satisfies flag.Value interface.
type LogLevel int32

const (
	// Note: reserving value zero to differentiate unspecified case.
	level_unspecified LogLevel = iota

	// DebugLevel is the lowest level of logging.
	// Debug logs are intended for debugging and development purposes.
	DebugLevel

	// InfoLevel is used for general informational log messages.
	InfoLevel

	// WarnLevel is used for undesired but relatively expected events,
	// which may indicate a problem.
	WarnLevel

	// ErrorLevel is used for undesired and unexpected events that
	// the program can recover from.
	ErrorLevel

	// FatalLevel is used for undesired and unexpected events that
	// the program cannot recover from.
	FatalLevel
)

// String is part of the flag.Value interface.
func (l *LogLevel) String() string {
	switch *l {
	case DebugLevel:
		return "debug"
	case InfoLevel:
		return "info"
	case WarnLevel:
		return "warn"
	case ErrorLevel:
		return "error"
	case FatalLevel:
		return "fatal"
	}
	panic(fmt.Sprintf("asynq: unexpected log level: %v", *l))
}

// Set is part of the flag.Value interface.
func (l *LogLevel) Set(val string) error {
	switch strings.ToLower(val) {
	case "debug":
		*l = DebugLevel
	case "info":
		*l = InfoLevel
	case "warn", "warning":
		*l = WarnLevel
	case "error":
		*l = ErrorLevel
	case "fatal":
		*l = FatalLevel
	default:
		return fmt.Errorf("asynq: unsupported log level %q", val)
	}
	return nil
}

func toInternalLogLevel(l LogLevel) log.Level {
	switch l {
	case DebugLevel:
		return log.DebugLevel
	case InfoLevel:
		return log.InfoLevel
	case WarnLevel:
		return log.WarnLevel
	case ErrorLevel:
		return log.ErrorLevel
	case FatalLevel:
		return log.FatalLevel
	}
	panic(fmt.Sprintf("asynq: unexpected log level: %v", l))
}

// DefaultRetryDelayFunc is the default RetryDelayFunc used if one is not specified in Config.
// It uses exponential back-off strategy to calculate the retry delay.
func DefaultRetryDelayFunc(n int, e error, t *Task) time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	// Formula taken from https://github.com/mperham/sidekiq.
	s := int(math.Pow(float64(n), 4)) + 15 + (r.Intn(30) * (n + 1))
	return time.Duration(s) * time.Second
}

func defaultIsFailureFunc(err error) bool { return err != nil }

var defaultQueueConfig = map[string]int{
	base.DefaultQueueName: 1,
}

const (
	defaultTaskCheckInterval        = 100 * time.Millisecond
	defaultShutdownTimeout          = 8 * time.Second
	defaultHealthCheckInterval      = 15 * time.Second
	defaultDelayedTaskCheckInterval = 5 * time.Second
	defaultGroupGracePeriod         = 1 * time.Minute
	defaultJanitorInterval          = 8 * time.Second
	defaultJanitorBatchSize         = 100
)

func NewRDB(cfg Config, cs ...redis.UniversalClient) *rdb.RDB {
	if cfg.RDB != nil {
		return cfg.RDB
	}
	return rdb.NewRDB(NewRClient(cfg, cs...))
}

func NewRClient(cfg Config, cs ...redis.UniversalClient) redis.UniversalClient {
	if cfg.RedisClientOpt.Addr == "" {
		cfg.RedisClientOpt = RedisClientOpt{Addr: "127.0.0.1:6379"}
	}
	if len(cs) > 0 {
		return cs[0]
	}
	c, ok := cfg.RedisClientOpt.MakeRedisClient().(redis.UniversalClient)
	if !ok {
		panic(fmt.Sprintf("asynq: unsupported RedisConnOpt type %T", cfg.RedisClientOpt))
	}
	return c
}

func NewServer(cfg Config) *Server {
	c := NewRClient(cfg)
	server := NewServerFromRedisClient(c, cfg)
	server.sharedConnection = false
	return server
}

// NewServerFromRedisClient returns a new Server given a redis connection option
// and server configuration.
func NewServerFromRedisClient(c redis.UniversalClient, cfg Config) *Server {
	rd := NewRDB(cfg)

	baseCtxFn := cfg.BaseContext
	if baseCtxFn == nil {
		baseCtxFn = context.Background
	}
	n := cfg.Concurrency
	if n < 1 {
		n = runtime.NumCPU()
	}

	taskCheckInterval := cfg.TaskCheckInterval
	if taskCheckInterval <= 0 {
		taskCheckInterval = defaultTaskCheckInterval
	}
	delayFunc := cfg.RetryDelayFunc
	if delayFunc == nil {
		delayFunc = DefaultRetryDelayFunc
	}
	isFailureFunc := cfg.IsFailure
	if isFailureFunc == nil {
		isFailureFunc = defaultIsFailureFunc
	}
	queues := make(map[string]int)
	for qname, p := range cfg.Queues {
		if err := base.ValidateQueueName(qname); err != nil {
			continue // ignore invalid queue names
		}
		if p > 0 {
			queues[qname] = p
		}
	}
	if len(queues) == 0 {
		queues = defaultQueueConfig
	}
	var qnames []string
	for q := range queues {
		qnames = append(qnames, q)
	}
	shutdownTimeout := cfg.ShutdownTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = defaultShutdownTimeout
	}
	healthcheckInterval := cfg.HealthCheckInterval
	if healthcheckInterval == 0 {
		healthcheckInterval = defaultHealthCheckInterval
	}
	// TODO: Create a helper to check for zero value and fall back to default (e.g. getDurationOrDefault())
	groupGracePeriod := cfg.GroupGracePeriod
	if groupGracePeriod == 0 {
		groupGracePeriod = defaultGroupGracePeriod
	}
	if groupGracePeriod < time.Second {
		panic("GroupGracePeriod cannot be less than a second")
	}
	logger := log.NewLogger(cfg.Logger)
	loglevel := cfg.LogLevel
	if loglevel == level_unspecified {
		loglevel = InfoLevel
	}
	logger.SetLevel(toInternalLogLevel(loglevel))
	starting := make(chan *workerInfo)
	finished := make(chan *base.TaskMessage)
	syncCh := make(chan *syncRequest)
	srvState := &serverState{value: srvStateNew}
	cancels := base.NewCancelations()

	taskStateProber := cfg.TaskStateProber
	if taskStateProber != nil {
		rd.SetTaskProber(*taskStateProber)
	}

	serverID := ""
	if cfg.ServerID != "" {
		serverID = cfg.ServerID
	} else {
		serverID = xid.New().String()
	}
	janitorInterval := cfg.JanitorInterval
	if janitorInterval == 0 {
		janitorInterval = defaultJanitorInterval
	}

	janitorBatchSize := cfg.JanitorBatchSize
	if janitorBatchSize == 0 {
		janitorBatchSize = defaultJanitorBatchSize
	}
	syncer := newSyncer(syncerParams{
		logger:     logger,
		requestsCh: syncCh,
		interval:   5 * time.Second,
	})
	heartbeater := newHeartbeater(heartbeaterParams{
		logger:         logger,
		broker:         rd,
		interval:       5 * time.Second,
		concurrency:    n,
		queues:         queues,
		serverID:       serverID,
		strictPriority: cfg.StrictPriority,
		state:          srvState,
		starting:       starting,
		finished:       finished,
	})
	delayedTaskCheckInterval := cfg.DelayedTaskCheckInterval
	if delayedTaskCheckInterval == 0 {
		delayedTaskCheckInterval = defaultDelayedTaskCheckInterval
	}
	forwarder := newForwarder(forwarderParams{
		logger:   logger,
		broker:   rd,
		queues:   qnames,
		interval: delayedTaskCheckInterval,
	})
	subscriber := newSubscriber(subscriberParams{
		logger:       logger,
		broker:       rd,
		cancelations: cancels,
	})
	processor := newProcessor(processorParams{
		logger:            logger,
		broker:            rd,
		taskCheckInterval: taskCheckInterval,
		retryDelayFunc:    delayFunc,
		baseCtxFn:         baseCtxFn,
		recoverPanicFunc:  cfg.RecoverPanicFunc,
		isFailureFunc:     isFailureFunc,
		syncCh:            syncCh,
		cancelations:      cancels,
		concurrency:       n,
		queues:            queues,
		strictPriority:    cfg.StrictPriority,
		errHandler:        cfg.ErrorHandler,
		completeHandler:   cfg.CompleteHandler,
		doneHandler:       cfg.DoneHandler,
		shutdownTimeout:   shutdownTimeout,
		starting:          starting,
		finished:          finished,
		serverID:          serverID,
	})
	recoverer := newRecoverer(recovererParams{
		logger:         logger,
		broker:         rd,
		retryDelayFunc: delayFunc,
		isFailureFunc:  isFailureFunc,
		queues:         qnames,
		interval:       1 * time.Minute,
	})
	healthchecker := newHealthChecker(healthcheckerParams{
		logger:          logger,
		broker:          rd,
		interval:        healthcheckInterval,
		healthcheckFunc: cfg.HealthCheckFunc,
	})
	janitor := newJanitor(janitorParams{
		logger:    logger,
		broker:    rd,
		queues:    qnames,
		interval:  janitorInterval,
		batchSize: janitorBatchSize,
	})
	aggregator := newAggregator(aggregatorParams{
		logger:          logger,
		broker:          rd,
		queues:          qnames,
		gracePeriod:     groupGracePeriod,
		maxDelay:        cfg.GroupMaxDelay,
		maxSize:         cfg.GroupMaxSize,
		groupAggregator: cfg.GroupAggregator,
	})
	return &Server{
		ServerID:         serverID,
		strictPriority:   cfg.StrictPriority,
		queues:           queues,
		sharedConnection: true,
		concurrency:      n,
		logger:           logger,
		broker:           rd,
		state:            srvState,
		forwarder:        forwarder,
		processor:        processor,
		syncer:           syncer,
		heartbeater:      heartbeater,
		subscriber:       subscriber,
		recoverer:        recoverer,
		healthchecker:    healthchecker,
		janitor:          janitor,
		aggregator:       aggregator,
	}
}

// A Handler processes tasks.
//
// ProcessTask should return nil if the processing of a task
// is successful.
//
// If ProcessTask returns a non-nil error or panics, the task
// will be retried after delay if retry-count is remaining,
// otherwise the task will be archived.
//
// One exception to this rule is when ProcessTask returns a SkipRetry error.
// If the returned error is SkipRetry or an error wraps SkipRetry, retry is
// skipped and the task will be immediately archived instead.
type Handler interface {
	ProcessTask(context.Context, *Task) Result
	GetType() string
	GetKey() string
}

type Result struct {
	Ctx    context.Context
	Status string `json:"status"`
	Data   []byte `json:"data"`
	Error  error  `json:"error"`
}

func (r Result) Unmarshal(data any) error {
	return json.Unmarshal(r.Data, data)
}

func (r Result) String() string {
	return string(r.Data)
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as a Handler. If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// Handler that calls f.
type HandlerFunc func(context.Context, *Task) Result

// ProcessTask calls fn(ctx, task)
func (fn HandlerFunc) ProcessTask(ctx context.Context, task *Task) Result {
	return fn(ctx, task)
}

// GetType c string
func (fn HandlerFunc) GetType() string {
	return "server"
}

// GetKey c string
func (fn HandlerFunc) GetKey() string {
	return ""
}

// ErrServerClosed indicates that the operation is now illegal because of the server has been shutdown.
var ErrServerClosed = errors.New("asynq: Server closed")

// Run starts the task processing and blocks until
// an os signal to exit the program is received. Once it receives
// a signal, it gracefully shuts down all active workers and other
// goroutines to process the tasks.
//
// Run returns any error encountered at server startup time.
// If the server has already been shutdown, ErrServerClosed is returned.
func (srv *Server) Run() error {
	if err := srv.Start(); err != nil {
		return err
	}
	srv.waitForSignals()
	srv.Shutdown()
	return nil
}

// Start starts the worker server. Once the server has started,
// it pulls tasks off queues and starts a worker goroutine for each task
// and then call Handler to process it.
// Tasks are processed concurrently by the workers up to the number of
// concurrency specified in Config.Concurrency.
//
// Start returns any error encountered at server startup time.
// If the server has already been shutdown, ErrServerClosed is returned.
func (srv *Server) Start() error {
	if srv.handler == nil {
		return fmt.Errorf("asynq: server cannot run with nil handler")
	}
	srv.processor.handler = srv.handler

	if err := srv.start(); err != nil {
		return err
	}
	srv.logger.Info("Starting processing")

	srv.heartbeater.start(&srv.wg)
	srv.healthchecker.start(&srv.wg)
	srv.subscriber.start(&srv.wg)
	srv.syncer.start(&srv.wg)
	srv.recoverer.start(&srv.wg)
	srv.forwarder.start(&srv.wg)
	srv.processor.start(&srv.wg)
	srv.janitor.start(&srv.wg)
	srv.aggregator.start(&srv.wg)
	return nil
}

// Checks server state and returns an error if pre-condition is not met.
// Otherwise it sets the server state to active.
func (srv *Server) start() error {
	srv.state.mu.Lock()
	defer srv.state.mu.Unlock()
	switch srv.state.value {
	case srvStateActive:
		return fmt.Errorf("asynq: the server is already running")
	case srvStateStopped:
		return fmt.Errorf("asynq: the server is in the stopped state. Waiting for shutdown.")
	case srvStateClosed:
		return ErrServerClosed
	}
	srv.state.value = srvStateActive
	return nil
}

// Shutdown gracefully shuts down the server.
// It gracefully closes all active workers. The server will wait for
// active workers to finish processing tasks for duration specified in Config.ShutdownTimeout.
// If worker didn't finish processing a task during the timeout, the task will be pushed back to Redis.
func (srv *Server) Shutdown() {
	srv.state.mu.Lock()
	if srv.state.value == srvStateNew || srv.state.value == srvStateClosed {
		srv.state.mu.Unlock()
		// server is not running, do nothing and return.
		return
	}
	srv.state.value = srvStateClosed
	srv.state.mu.Unlock()

	srv.logger.Info("Starting graceful shutdown")
	// Note: The order of shutdown is important.
	// Sender goroutines should be terminated before the receiver goroutines.
	// processor -> syncer (via syncCh)
	// processor -> heartbeater (via starting, finished channels)
	srv.forwarder.shutdown()
	srv.processor.shutdown()
	srv.recoverer.shutdown()
	srv.syncer.shutdown()
	srv.subscriber.shutdown()
	srv.janitor.shutdown()
	srv.aggregator.shutdown()
	srv.healthchecker.shutdown()
	srv.heartbeater.shutdown()
	srv.wg.Wait()
	if !srv.sharedConnection {
		srv.broker.Close()
	}
	srv.logger.Info("Exiting")
}

// Stop signals the server to stop pulling new tasks off queues.
// Stop can be used before shutting down the server to ensure that all
// currently active tasks are processed before server shutdown.
//
// Stop does not shutdown the server, make sure to call Shutdown before exit.
func (srv *Server) Stop() {
	srv.state.mu.Lock()
	if srv.state.value != srvStateActive {
		// Invalid call to Stop, server can only go from Active state to Stopped state.
		srv.state.mu.Unlock()
		return
	}
	srv.state.value = srvStateStopped
	srv.state.mu.Unlock()

	srv.logger.Info("Stopping processor")
	srv.processor.stop()
	srv.logger.Info("Processor stopped")
}

func (srv *Server) AddHandler(handler *ServeMux) {
	srv.handler = handler
}

func (srv *Server) AddQueueHandler(queue string, handler func(ctx context.Context, t *Task) Result) {
	srv.handler.HandleFunc(queue, handler)
}

func (srv *Server) RemoveQueueHandler(queue string) {
	srv.handler.Remove(queue)
}

func remove[T any](l []T, remove func(T) bool) []T {
	out := make([]T, 0)
	for _, element := range l {
		if !remove(element) {
			out = append(out, element)
		}
	}
	return out
}

func (srv *Server) AddQueues(queues map[string]int) {
	srv.queues = queues
	for queue := range srv.queues {
		srv.forwarder.queues = append(srv.forwarder.queues, queue)
		srv.recoverer.queues = append(srv.recoverer.queues, queue)
	}

	srv.heartbeater.queues = srv.queues
	srv.processor.queueConfig = srv.queues
	ques, orderedQueues := prepareQueues(srv.processor.queueConfig, srv.strictPriority)
	srv.processor.queueConfig = ques
	srv.processor.orderedQueues = orderedQueues
}

func (srv *Server) AddQueue(queue string, prio ...int) {
	priority := 0
	if len(prio) > 0 {
		priority = prio[0]
	}
	srv.queues[queue] = priority
	srv.heartbeater.queues = srv.queues
	srv.forwarder.queues = append(srv.forwarder.queues, queue)
	srv.processor.queueConfig[queue] = priority
	queues, orderedQueues := prepareQueues(srv.processor.queueConfig, srv.strictPriority)
	srv.processor.queueConfig = queues
	srv.processor.orderedQueues = orderedQueues
	srv.recoverer.queues = append(srv.recoverer.queues, queue)
}

func (srv *Server) RemoveQueue(queue string) {
	var qName []string
	delete(srv.queues, queue)
	for queue := range srv.queues {
		qName = append(qName, queue)
	}
	srv.heartbeater.queues = srv.queues
	srv.forwarder.queues = qName
	srv.processor.queueConfig = srv.queues
	queues, orderedQueues := prepareQueues(srv.processor.queueConfig, srv.strictPriority)
	srv.processor.queueConfig = queues
	srv.processor.orderedQueues = orderedQueues
	srv.recoverer.queues = qName
}

func (srv *Server) HasQueue(queueName string) bool {
	for _, que := range srv.forwarder.queues {
		if que != queueName {
			return true
		}
	}
	return false
}

func (srv *Server) Tune(concurrency int) {
	srv.concurrency = concurrency
	srv.heartbeater.concurrency = concurrency
	srv.processor.sema = make(chan struct{}, concurrency)
}

func (srv *Server) IsRunning() bool {
	return srv.state.value == srvStateActive
}

func (srv *Server) IsStopped() bool {
	return srv.state.value == srvStateStopped
}

func (srv *Server) IsClosed() bool {
	return srv.state.value == srvStateClosed
}

// SetTaskStateProber StateChanged watch state updates, with more customized detail
func (srv *Server) SetTaskStateProber(prober base.TaskProber) {
	srv.broker.SetTaskProber(prober)
}
