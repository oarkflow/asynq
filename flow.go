package asynq

/*
import (
	"context"
	"errors"
	json "github.com/bytedance/sonic"
	"strings"
	"sync"

	"github.com/oarkflow/xid"
	"golang.org/x/sync/errgroup"

	"github.com/oarkflow/asynq/internal/base"
	"github.com/oarkflow/asynq/internal/rdb"
)

type Branch struct {
	Key              string            `json:"key"`
	ConditionalNodes map[string]string `json:"conditional_nodes"`
}

type Mode string

const (
	Sync  Mode = "sync"
	Async Mode = "async"
	Form  Mode = "form"
)

type Flow struct {
	ID                 string            `json:"id"`
	Name               string            `json:"name"`
	Slug               string            `json:"slug"`
	Mode               Mode              `json:"mode"`
	Error              error             `json:"error"`
	Nodes              []string          `json:"nodes,omitempty"`
	Edges              [][]string        `json:"edges,omitempty"`
	Loops              [][]string        `json:"loops,omitempty"`
	UserID             any               `json:"user_id"`
	Branches           []Branch          `json:"branches,omitempty"`
	LastNode           string            `json:"last_node"`
	FirstNode          string            `json:"first_node"`
	RedisAddress       string            `json:"redis_address"`
	EnableTaskGrouping bool              `json:"enable_task_grouping"`
	Status             string            `json:"status"`
	CronEntries        map[string]string `json:"cron_entries"`
	nodeHandler        map[string]Handler
	rdb                *rdb.RDB
	Config             Config
	server             *Server
	handler            *ServeMux
	inspector          *Inspector
	scheduler          *Scheduler
	edges              map[string][]string
	loops              map[string][]string
	branches           map[string]map[string]string
	mu                 sync.Mutex
}

type CronReportHandler struct {
	Key  string `json:"key"`
	Type string `json:"type"`
	flow *Flow
}

func (v *CronReportHandler) ProcessTask(ctx context.Context, task *Task) Result {
	return Result{}
}

func (v *CronReportHandler) GetType() string {
	return v.Type
}

func (v *CronReportHandler) GetKey() string {
	return v.Key
}

type HandleFinalStatus struct {
	rdb      *rdb.RDB
	flow     *Flow
	RedisUri string
	config   Config
}

func (fn *HandleFinalStatus) handle(payload []byte, flowID, operation, status string) {
	if fn.rdb == nil {
		fn.rdb = NewRDB(Config{RedisClientOpt: RedisClientOpt{Addr: fn.RedisUri}})
	}
	data := make(map[string]any)
	src := make(map[string]any)
	json.Unmarshal(payload, &data)
	if id, ok := data[fn.config.idKey]; ok {
		d, _ := fn.rdb.Client().Get(context.Background(), "o:f:"+flowID+":t:"+id.(string)).Bytes()
		if d != nil {
			json.Unmarshal(d, &src)
			data = MergeMap(src, data)
		}
		data[fn.config.operationKey] = operation
		data[fn.config.statusKey] = status
		dataToWrite, _ := json.Marshal(data)
		fn.rdb.Client().Set(context.Background(), "o:f:"+flowID+":t:"+id.(string), dataToWrite, 0)
		fn.rdb.Client().RPush(context.Background(), "o:f:"+flowID+":o:"+operation, dataToWrite)
	}
}

func (fn *HandleFinalStatus) HandleComplete(ctx context.Context, task *Task) {
	fn.handle(task.Payload(), task.FlowID, task.Type(), "completed")
}

func (fn *HandleFinalStatus) HandleDone(ctx context.Context, task *Task) {
	fn.handle(task.Payload(), task.FlowID, task.Type(), "completed")
}

func (fn *HandleFinalStatus) HandleError(ctx context.Context, task *Task, err error) {
	fn.handle(task.Payload(), task.FlowID, task.Type(), "failed")
}

func NewFlow(config ...Config) *Flow {
	var cfg Config
	if len(config) > 0 {
		cfg = config[0]
	}
	if cfg.RedisServer != "" {
		cfg.RedisClientOpt = RedisClientOpt{Addr: cfg.RedisServer}
	} else {
		cfg.Mode = Sync
	}

	if cfg.Mode == "" {
		cfg.Mode = Async
	}
	if cfg.FlowPrefix == "" {
		cfg.FlowPrefix = "asynq"
	}
	cfg.idKey = cfg.FlowPrefix + "_id"
	cfg.operationKey = cfg.FlowPrefix + "_operation"
	cfg.statusKey = cfg.FlowPrefix + "_status"
	cfg.flowIDKey = cfg.FlowPrefix + "_flow_id"
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 1
	}
	if cfg.FlowID == "" {
		cfg.FlowID = xid.New().String()
	}
	if cfg.ServerID == "" {
		cfg.ServerID = cfg.FlowID
	}

	f := &Flow{
		ID:           cfg.FlowID,
		Mode:         cfg.Mode,
		UserID:       cfg.UserID,
		nodeHandler:  make(map[string]Handler),
		RedisAddress: cfg.RedisServer,
		CronEntries:  make(map[string]string),
		mu:           sync.Mutex{},
		edges:        make(map[string][]string),
		loops:        make(map[string][]string),
		branches:     make(map[string]map[string]string),
	}

	if cfg.Mode == Async {
		cfg.RDB = NewRDB(cfg)
		f.rdb = cfg.RDB
		f.inspector = NewInspectorFromRDB(cfg.RDB)
		f.scheduler = NewSchedulerFromRDB(cfg.RDB, nil)
		if cfg.CompleteHandler == nil {
			cfg.CompleteHandler = &HandleFinalStatus{RedisUri: cfg.RedisServer, flow: f, config: cfg}
		}
		if cfg.DoneHandler == nil {
			cfg.DoneHandler = &HandleFinalStatus{RedisUri: cfg.RedisServer, flow: f, config: cfg}
		}
		if cfg.ErrorHandler == nil {
			cfg.ErrorHandler = &HandleFinalStatus{RedisUri: cfg.RedisServer, flow: f, config: cfg}
		}
		f.server = NewServer(cfg)
	}
	f.Config = cfg

	return f
}

func (f *Flow) QueueList() ([]*QueueInfo, error) {
	queueList, err := f.inspector.Queues()
	if err != nil {
		return nil, err
	}
	var queues []*QueueInfo
	for _, queue := range queueList {
		if strings.Contains(queue, f.ID) {
			info, err := f.QueueInfo(queue)
			if err != nil {
				return nil, err
			}
			queues = append(queues, info)
		}
	}
	return queues, nil
}

func (f *Flow) QueueHistory(queue string, noOfDays int) ([]*DailyStats, error) {
	if noOfDays == 0 {
		noOfDays = 7
	}
	return f.inspector.History(queue, noOfDays)
}

func (f *Flow) QueueInfo(queue string) (*QueueInfo, error) {
	return f.inspector.GetQueueInfo(queue)
}

func (f *Flow) Pause(queue string) error {
	return f.inspector.PauseQueue(queue)
}

func (f *Flow) Unpause(queue string) error {
	return f.inspector.UnpauseQueue(queue)
}

func (f *Flow) TaskListByStatus(queue string, status string) ([]*TaskInfo, error) {
	switch status {
	case "Active":
		return f.inspector.ListActiveTasks(queue)
	case "Pending":
		return f.inspector.ListPendingTasks(queue)
	case "Scheduled":
		return f.inspector.ListScheduledTasks(queue)
	case "Archived":
		return f.inspector.ListArchivedTasks(queue)
	case "Retry":
		return f.inspector.ListRetryTasks(queue)
	default:
		return nil, nil
	}
}

func (f *Flow) ActiveTaskList(queue string) ([]*TaskInfo, error) {
	return f.inspector.ListActiveTasks(queue)
}

func (f *Flow) PendingTaskList(queue string) ([]*TaskInfo, error) {
	return f.inspector.ListPendingTasks(queue)
}

func (f *Flow) ScheduledTaskList(queue string) ([]*TaskInfo, error) {
	return f.inspector.ListScheduledTasks(queue)
}

func (f *Flow) ArchivedTaskList(queue string) ([]*TaskInfo, error) {
	return f.inspector.ListArchivedTasks(queue)
}

func (f *Flow) RetryTaskList(queue string) ([]*TaskInfo, error) {
	return f.inspector.ListRetryTasks(queue)
}

func (f *Flow) Enqueue(ctx context.Context, queueName string, broker base.Broker, flowID string, payload []byte, result *Result) {
	task := NewTask(queueName, payload, FlowID(flowID), Queue(queueName))
	_, err := EnqueueContext(broker, ctx, task, FlowID(flowID), Queue(queueName))
	if err != nil {
		result.Error = err
	}
}

func (f *Flow) loop(ctx context.Context, task *Task) ([]any, error) {
	extraParams := map[string]any{}
	g, ctx := errgroup.WithContext(ctx)
	ep := ctx.Value("extra_params")
	switch ep := ep.(type) {
	case map[string]any:
		extraParams = ep
	case string:
		json.Unmarshal([]byte(ep), &extraParams)
	}
	result := make(chan interface{})
	var rs, results []interface{}
	err := json.Unmarshal(task.Payload(), &rs)
	if err != nil {
		return nil, err
	}
	for _, single := range rs {
		single := single
		g.Go(func() error {
			var payload []byte
			currentData := make(map[string]any)
			switch s := single.(type) {
			case map[string]any:
				if len(extraParams) > 0 {
					for k, v := range extraParams {
						s[k] = v
					}
				}
				id := xid.New().String()
				if _, ok := s[f.Config.idKey]; !ok {
					s[f.Config.idKey] = id
				}
				if _, ok := s[f.Config.flowIDKey]; !ok {
					s[f.Config.flowIDKey] = f.ID
				}
				if _, ok := s[f.Config.statusKey]; !ok {
					s[f.Config.statusKey] = "pending"
				}
				currentData = s
				payload, err = json.Marshal(currentData)
				if err != nil {
					return err
				}
				break
			default:
				payload, err = json.Marshal(single)
				if err != nil {
					return err
				}
			}
			var responseData map[string]interface{}
			if ft, ok := f.loops[task.Type()]; ok {
				for _, v := range ft {
					t := NewTask(v, payload, FlowID(task.FlowID), Queue(v))
					res := f.processTask(ctx, t, f.nodeHandler[v])
					if res.Error != nil {
						if f.Config.ErrorHandler != nil {
							f.Config.ErrorHandler.HandleError(ctx, task, res.Error)
						}
						return res.Error
					} else {
						if f.Config.CompleteHandler != nil {
							f.Config.CompleteHandler.HandleComplete(ctx, task)
						}
					}
					err = json.Unmarshal(res.Data, &responseData)
					if err != nil {
						return err
					}
					currentData = MergeMap(currentData, responseData)
				}
				payload, err = json.Marshal(currentData)
				if err != nil {
					return err
				}
				err = json.Unmarshal(payload, &single)
				if err != nil {
					return err
				}
				select {
				case result <- single:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	}
	go func() {
		g.Wait()
		close(result)
	}()
	for ch := range result {
		results = append(results, ch)
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

func (f *Flow) processTask(ctx context.Context, task *Task, handler ...Handler) Result {
	var h Handler
	if len(handler) > 0 && handler[0] != nil {
		h = handler[0]
	} else if f.FirstNode != "" {
		h = f.nodeHandler[f.FirstNode]
	}
	if h == nil {
		return Result{Error: errors.New("Handler not found")}
	}
	result := h.ProcessTask(ctx, task)
	if result.Error != nil {
		if f.Config.ErrorHandler != nil {
			f.Config.ErrorHandler.HandleError(ctx, task, result.Error)
		}
		return result
	} else {
		if f.Config.CompleteHandler != nil {
			f.Config.CompleteHandler.HandleComplete(ctx, task)
		}
	}
	if h.GetType() == "loop" {
		var c context.Context
		if result.Ctx != nil {
			c = result.Ctx
		} else {
			c = ctx
		}
		newTask := NewTask(task.Type(), result.Data, FlowID(f.ID))
		results, err := f.loop(c, newTask)
		if err != nil {
			result.Error = err
			return result
		}
		tmp, err := json.Marshal(results)
		if err != nil {
			result.Error = err
			return result
		}
		result.Data = tmp
	}
	if h.GetType() == "condition" {
		var ct context.Context
		if result.Ctx != nil {
			ct = result.Ctx
		} else {
			ct = ctx
		}
		if ft, ok := f.branches[task.Type()]; ok && result.Status != "" {
			if c, o := ft[result.Status]; o {
				t := NewTask(c, result.Data, FlowID(task.FlowID))
				res := f.processTask(ct, t, f.nodeHandler[c])
				if res.Error != nil {
					if f.Config.ErrorHandler != nil {
						f.Config.ErrorHandler.HandleError(ct, task, res.Error)
					}
					return res
				} else {
					if f.Config.CompleteHandler != nil {
						f.Config.CompleteHandler.HandleComplete(ct, task)
					}
				}
				result = res
			}
		}
	}
	if ft, ok := f.edges[task.Type()]; ok {
		var ct context.Context
		if result.Ctx != nil {
			ct = result.Ctx
		} else {
			ct = ctx
		}
		for _, v := range ft {
			t := NewTask(v, result.Data, FlowID(task.FlowID))
			res := f.processTask(ct, t, f.nodeHandler[v])
			if res.Error != nil {
				if f.Config.ErrorHandler != nil {
					f.Config.ErrorHandler.HandleError(ct, task, res.Error)
				}
				return res
			} else {
				if f.Config.CompleteHandler != nil {
					f.Config.CompleteHandler.HandleComplete(ct, task)
				}
			}
			result = res
		}
	}
	return result
}

func (f *Flow) ProcessTask(ctx context.Context, task *Task) Result {
	return f.processTask(ctx, task)
}

func (f *Flow) GetType() string {
	return "flow"
}

func (f *Flow) GetKey() string {
	return f.Config.FlowPrefix
}

func (f *Flow) edgeMiddleware(h Handler) Handler {
	return HandlerFunc(func(ctx context.Context, task *Task) Result {
		result := h.ProcessTask(ctx, task)
		if result.Error != nil {
			return result
		}
		if h.GetType() == "loop" {
			var rs []any
			err := json.Unmarshal(result.Data, &rs)
			if err != nil {
				result.Error = err
				return result
			}
			for _, single := range rs {
				single := single
				payload := result.Data
				currentData := make(map[string]any)
				switch s := single.(type) {
				case map[string]any:
					id := xid.New().String()
					if _, ok := s[f.Config.idKey]; !ok {
						s[f.Config.idKey] = id
					}
					if _, ok := s[f.Config.flowIDKey]; !ok {
						s[f.Config.flowIDKey] = task.FlowID
					}
					if _, ok := s[f.Config.statusKey]; !ok {
						s[f.Config.statusKey] = "pending"
					}
					currentData = s
					payload, err = json.Marshal(currentData)
					if err != nil {
						result.Error = err
						return result
					}
					err = task.ResultWriter().Broker().AddTask("o:f:"+task.FlowID+":t:"+id, payload)
					if err != nil {
						result.Error = err
						return result
					}
					break
				default:
					payload, err = json.Marshal(single)
					if err != nil {
						result.Error = err
						return result
					}
				}
				if ft, ok := f.loops[task.Type()]; ok {
					for _, v := range ft {
						f.Enqueue(ctx, v, task.ResultWriter().Broker(), task.FlowID, payload, &result)
						if result.Error != nil {
							return result
						}
					}
				}
			}
		}
		if h.GetType() == "condition" {
			if ft, ok := f.branches[task.Type()]; ok && result.Status != "" {
				if c, o := ft[result.Status]; o {
					f.Enqueue(ctx, c, task.ResultWriter().Broker(), task.FlowID, result.Data, &result)
					if result.Error != nil {
						return result
					}
				}
			}
		}
		if ft, ok := f.edges[task.Type()]; ok {
			for _, v := range ft {
				f.Enqueue(ctx, v, task.ResultWriter().Broker(), task.FlowID, result.Data, &result)
				if result.Error != nil {
					return result
				}
			}
		}
		return result
	})
}

func (f *Flow) AddHandler(node string, handler Handler) *Flow {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodeHandler[node] = handler
	f.Nodes = append(f.Nodes, node)
	return f
}

func (f *Flow) AddEdge(in, out string) *Flow {
	edge := []string{in, out}
	f.Edges = append(f.Edges, edge)
	return f
}

func (f *Flow) AddBranch(vertex string, conditions map[string]string) *Flow {
	branch := Branch{
		Key:              vertex,
		ConditionalNodes: conditions,
	}
	f.Branches = append(f.Branches, branch)
	return f
}

func (f *Flow) AddLoop(in string, out ...string) *Flow {
	loop := []string{in}
	loop = append(loop, out...)
	f.Loops = append(f.Loops, loop)
	return f
}

func (f *Flow) Prepare() *Flow {
	for _, edge := range f.Edges {
		f.edges[edge[0]] = append(f.edges[edge[0]], edge[1])
	}
	for _, loop := range f.Loops {
		f.loops[loop[0]] = append(f.loops[loop[0]], loop[1:]...)
	}
	for _, branch := range f.Branches {
		f.branches[branch.Key] = branch.ConditionalNodes
	}
	if f.FirstNode == "" {
		for node, handler := range f.nodeHandler {
			if handler.GetType() == "input" {
				f.FirstNode = node
			}
			if handler.GetType() == "output" {
				f.LastNode = node
			}
		}
	}
	return f
}

func (f *Flow) SetupServer() error {
	if f.Config.NoService {
		return nil
	}
	f.Prepare()
	mux := NewServeMux()
	for node, handler := range f.nodeHandler {
		if f.Mode == Async {
			f.server.AddQueue(node, 1)
			f.rdb.Client().SAdd(context.Background(), base.AllQueues, node)
			result := mux.Handle(node, handler)
			if result.Error != nil {
				return result.Error
			}
		}
	}

	key := "cron:1:" + f.ID
	if f.Config.CronReportHandler == nil {
		mux.Handle(key, &CronReportHandler{flow: f})
	} else {
		mux.Handle(key, f.Config.CronReportHandler)
	}
	if f.Mode == Async {
		f.server.AddQueue(key, 2)

		mux.Use(f.edgeMiddleware)
		f.handler = mux
		f.server.AddHandler(mux)
		register, err := f.scheduler.Register("@every 10s", NewTask(key, nil, FlowID(f.ID), Queue(key)), Queue(key))
		if err != nil {
			return err
		}
		f.mu.Lock()
		f.CronEntries[register] = key
		f.mu.Unlock()
	}
	return nil
}

func (f *Flow) Use(handler func(h Handler) Handler) {
	if f.handler != nil {
		f.handler.Use(handler)
	}
}

func (f *Flow) Run() error {
	if f.server == nil {
		return nil
	}
	return f.server.Run()
}

func (f *Flow) Start() error {
	if f.server == nil {
		return nil
	}
	if f.scheduler.state.value != srvStateActive {
		f.scheduler.Start()
	}

	if f.server.state.value != srvStateActive {
		return f.server.Start()
	}
	return nil
}

func (f *Flow) GetStatus() string {
	if f.server == nil {
		f.Status = "new"
		return f.Status
	}
	switch f.server.state.value {
	case srvStateActive:
		f.Status = "active"
		break
	case srvStateStopped:
		f.Status = "stopped"
		break
	case srvStateClosed:
		f.Status = "closed"
		break
	default:
		f.Status = "new"
		break
	}
	return f.Status
}

func (f *Flow) Shutdown() {
	if f.server == nil {
		return
	}
	f.scheduler.Shutdown()
	f.server.Shutdown()
}

func (f *Flow) Send(data []byte) Result {
	if f.Mode == Async {
		res, err := f.SendAsync(data)
		if err != nil {
			return Result{Error: err}
		}
		result := Result{
			Status: res.State.String(),
			Data:   res.Payload,
		}
		return result
	}
	task := NewTask(f.FirstNode, data, FlowID(f.ID))
	return f.processTask(context.Background(), task, f.nodeHandler[f.FirstNode])
}

func (f *Flow) SendAsync(data []byte) (*TaskInfo, error) {
	return SendAsync(f.RedisAddress, f.ID, f.FirstNode, data)
}

func SendAsync(redisAddress, flowID string, queue string, data []byte) (*TaskInfo, error) {
	task := NewTask(queue, data, FlowID(flowID))
	client := NewClient(RedisClientOpt{Addr: redisAddress})
	defer client.Close()
	var ops []Option
	ops = append(ops, Queue(queue), FlowID(flowID))
	return client.Enqueue(task, ops...)
}

func MergeMap(map1 map[string]any, map2 map[string]any) map[string]any {
	for k, m := range map2 {
		if _, ok := map1[k]; !ok {
			map1[k] = m
		}
	}
	return map1
}
*/
