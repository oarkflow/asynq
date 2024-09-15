package asynq

import (
	"context"
	"errors"
	"sync"

	"github.com/oarkflow/json"

	"github.com/oarkflow/xid"
	"golang.org/x/sync/errgroup"

	"github.com/oarkflow/asynq/base"
	"github.com/oarkflow/asynq/rdb"
)

type Mode string

const (
	Sync  Mode = "sync"
	Async Mode = "async"
)

type node struct {
	id      string
	handler Handler
	params  map[string]any
	loops   []string
	edges   []string
	flow    *Flow
}

func (n *node) loop(ctx context.Context, payload []byte) ([]any, error) {
	g, ctx := errgroup.WithContext(ctx)
	extraParams := getExtraParams(ctx)
	result := make(chan any)
	var rs, results []any
	err := json.Unmarshal(payload, &rs)
	if err != nil {
		return nil, NewFlowError(err, n.GetKey(), n.GetType())
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
				if _, ok := s[n.flow.config.idKey]; !ok {
					s[n.flow.config.idKey] = id
				}
				if _, ok := s[n.flow.config.flowIDKey]; !ok {
					s[n.flow.config.flowIDKey] = n.flow.ID
				}
				if _, ok := s[n.flow.config.statusKey]; !ok {
					s[n.flow.config.statusKey] = "pending"
				}
				currentData = s
				payload, err = json.Marshal(currentData)
				if err != nil {
					fError := NewFlowError(err, n.GetKey(), n.GetType())
					result <- fError
					return fError
				}
				break
			default:
				payload, err = json.Marshal(single)
				if err != nil {
					fError := NewFlowError(err, n.GetKey(), n.GetType())
					result <- fError
					return fError
				}
			}
			var responseData map[string]interface{}
			for _, v := range n.loops {
				t := NewTask(v, payload, FlowID(n.flow.ID), Queue(v))
				res := n.flow.processNode(ctx, t, n.flow.nodes[v])
				if res.Error != nil {
					fError := NewFlowError(res.Error, n.GetKey(), n.GetType())
					result <- fError
					return fError
				}
				err = json.Unmarshal(res.Data, &responseData)
				if err != nil {
					fError := NewFlowError(err, n.GetKey(), n.GetType())
					result <- fError
					return fError
				}
				currentData = mergeMap(currentData, responseData)
			}
			payload, err = json.Marshal(currentData)
			if err != nil {
				fError := NewFlowError(err, n.GetKey(), n.GetType())
				result <- fError
				return fError
			}
			err = json.Unmarshal(payload, &single)
			if err != nil {
				fError := NewFlowError(err, n.GetKey(), n.GetType())
				result <- fError
				return fError
			}
			select {
			case result <- single:
			case <-ctx.Done():
				result <- ctx.Err()
			}
			return nil
		})
	}
	go func() {
		_ = g.Wait()
		close(result)
	}()
	for ch := range result {
		switch ch := ch.(type) {
		case error:
			if ch != nil {
				return results, ch
			}
		default:
			results = append(results, ch)
		}
	}
	return results, nil
}

func (n *node) ProcessTask(ctx context.Context, task *Task) Result {
	var c context.Context
	result := n.handler.ProcessTask(ctx, task)
	if result.Error != nil {
		result.Error = NewFlowError(result.Error, n.GetKey(), n.GetType())
		return result
	}
	if result.Ctx != nil {
		c = result.Ctx
	} else {
		c = ctx
	}
	if n.flow.Mode == Sync && len(n.loops) > 0 {
		arr, err := n.loop(c, result.Data)
		if err != nil {
			result.Error = NewFlowError(err, n.GetKey(), n.GetType())
			return result
		}
		bt, err := json.Marshal(arr)
		result.Data = bt
		if err != nil {
			result.Error = NewFlowError(err, n.GetKey(), n.GetType())
		}
	}

	return result
}

func (n *node) GetType() string {
	return n.handler.GetType()
}

func (n *node) SetPayload(_ Payload) {

}

func (n *node) SetKey(key string) {
	n.id = key
}

func (n *node) GetKey() string {
	return n.id
}

type Flow struct {
	Key         string                       `json:"key"`
	ID          string                       `json:"id"`
	Mode        Mode                         `json:"mode"`
	UserID      any                          `json:"user_id"`
	FirstNode   string                       `json:"first_node"`
	Nodes       []string                     `json:"nodes"`
	Edges       [][]string                   `json:"edges"`
	Loops       [][]string                   `json:"loops"`
	Branches    map[string]map[string]string `json:"branches"`
	Error       error                        `json:"error"`
	Status      string                       `json:"status"`
	CronEntries map[string]string            `json:"cron_entries"`

	rdb       *rdb.RDB
	server    *Server
	handler   *ServeMux
	inspector *Inspector
	scheduler *Scheduler

	config    Config
	nodes     map[string]*node
	firstNode *node
	path      [][]string
	mu        sync.RWMutex
	prepared  bool
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
		Mode:        cfg.Mode,
		ID:          cfg.FlowID,
		UserID:      cfg.UserID,
		nodes:       make(map[string]*node),
		Branches:    make(map[string]map[string]string),
		CronEntries: make(map[string]string),
		mu:          sync.RWMutex{},
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
	f.config = cfg
	return f
}

func (f *Flow) GetHandler(id string) Handler {
	return f.nodes[id]
}

func (f *Flow) AddHandler(id string, handler Handler, params ...map[string]any) *Flow {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := &node{
		id:      id,
		handler: handler,
		flow:    f,
	}
	if len(params) > 0 {
		n.params = params[0]
	}
	handler.SetKey(id)
	f.nodes[id] = n
	f.Nodes = append(f.Nodes, id)
	return f
}

func (f *Flow) AddEdge(in, out string) *Flow {
	edge := []string{in, out}
	f.Edges = append(f.Edges, edge)
	return f
}

func (f *Flow) AddLoop(in string, out ...string) *Flow {
	loop := []string{in}
	loop = append(loop, out...)
	f.Loops = append(f.Loops, loop)
	return f
}

func (f *Flow) AddBranch(id string, branch map[string]string) *Flow {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Branches[id] = branch
	return f
}

func (f *Flow) processBranches(c context.Context, d []byte, n *node) Result {
	var result Result
	var r map[string]any
	err := json.Unmarshal(d, &r)
	if err != nil {
		result.Error = err
		f.Error = result.Error
		return result
	}
	g, ctx := errgroup.WithContext(c)
	edgeResult := make(map[string][]byte)
	rt := make(chan any)
	for handler, data := range r {
		bt, _ := json.Marshal(data)
		if nd, ok := f.nodes[handler]; ok {
			edge := handler
			nd := nd
			g.Go(func() error {
				newTask := NewTask(edge, bt, FlowID(n.flow.ID), Queue(edge))
				r := f.processNode(c, newTask, nd)
				if r.Error != nil {
					rt <- r.Error
					return r.Error
				}
				select {
				case rt <- map[string]Result{edge: r}:
				case <-ctx.Done():
					rt <- ctx.Err()
				}
				return nil
			})

		}
	}

	go func() {
		_ = g.Wait()
		close(rt)
	}()
	for ch := range rt {
		switch ch := ch.(type) {
		case error:
			if ch != nil {
				result.Error = ch
				f.Error = result.Error
				return result
			}
		case map[string]Result:
			for edge, r := range ch {
				edgeResult[edge+"_result"] = r.Data
			}
		}
	}
	data := make(map[string]any)

	// add extra params to the result
	extraParams := getExtraParams(ctx)
	if len(extraParams) > 0 {
		for k, v := range extraParams {
			data[k] = v
		}
	}
	for key, val := range edgeResult {
		var d any
		err := json.Unmarshal(val, &d)
		if err != nil {
			result.Error = err
			f.Error = result.Error
			return result
		}
		data[key] = d
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		result.Error = err
		f.Error = result.Error
		return result
	}
	result.Data = bytes
	return result
}

func (f *Flow) processEdges(c context.Context, result Result, n *node) Result {
	g, ctx := errgroup.WithContext(c)
	edgeResult := make(map[string][]byte)
	rt := make(chan any)
	for _, edge := range n.edges {
		if nd, ok := f.nodes[edge]; ok {
			edge := edge
			nd := nd
			g.Go(func() error {
				newTask := NewTask(edge, result.Data, FlowID(n.flow.ID), Queue(edge))
				r := f.processNode(c, newTask, nd)
				if r.Error != nil {
					rt <- r.Error
				}
				select {
				case rt <- map[string]Result{edge: r}:
				case <-ctx.Done():
					rt <- ctx.Err()
				}
				return nil
			})

		}
	}

	go func() {
		_ = g.Wait()
		close(rt)
	}()
	for ch := range rt {
		switch ch := ch.(type) {
		case error:
			if ch != nil {
				result.Error = ch
				f.Error = result.Error
				return result
			}
		case map[string]Result:
			for edge, r := range ch {
				edgeResult[edge+"_result"] = r.Data
			}
		}
	}
	totalResults := len(edgeResult)
	for _, r := range edgeResult {
		if totalResults == 1 {
			result.Data = r
			return result
		}
	}
	if totalResults == 0 {
		return result
	}
	edgeResult[n.id+"_result"] = result.Data
	data := make(map[string]any)

	// add extra params to the result
	extraParams := getExtraParams(ctx)
	if len(extraParams) > 0 {
		for k, v := range extraParams {
			data[k] = v
		}
	}

	for key, val := range edgeResult {
		d, _, err := asMap(val)
		if err != nil {
			result.Error = err
			return result
		}
		data[key] = d
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		result.Error = err
		return result
	}
	result.Data = bytes
	return result
}

func (f *Flow) processNode(ctx context.Context, task *Task, n *node) Result {
	result := n.ProcessTask(ctx, task)
	if result.Error != nil {
		f.Error = result.Error
		return result
	}
	var c context.Context
	if result.Ctx != nil {
		c = result.Ctx
	} else {
		c = ctx
	}
	if n.GetType() == "condition" {
		if ft, ok := f.Branches[n.id]; ok && result.Status != "" {
			if cy, o := ft[result.Status]; o {
				t := NewTask(cy, result.Data, FlowID(f.ID))
				result = f.processNode(c, t, f.nodes[cy])
				if result.Error != nil {
					f.Error = result.Error
					return result
				}
			}
		} else if result.Status == "branches" {
			result = f.processBranches(c, result.Data, n)
			if result.Error != nil {
				f.Error = result.Error
				return result
			}
		}
	}
	return f.processEdges(c, result, n)
}

func (f *Flow) ProcessTask(ctx context.Context, task *Task) Result {
	f.prepareNodes()
	if f.firstNode == nil {
		return Result{Error: errors.New("provide initial handler")}
	}
	return f.processNode(ctx, task, f.firstNode)
}

func (f *Flow) GetType() string {
	return "flow"
}

func (f *Flow) GetKey() string {
	return f.Key
}

func (f *Flow) prepareNodes() {
	if f.prepared {
		return
	}
	var src, dest []string
	for _, edge := range f.Edges {
		in := edge[0]
		out := edge[1]
		src = append(src, in)
		dest = append(dest, out)
		if node, ok := f.nodes[in]; ok {
			node.edges = append(node.edges, out)
		}
	}
	for _, loop := range f.Loops {
		in := loop[0]
		out := loop[1:]
		src = append(src, in)
		dest = append(dest, out...)
		if node, ok := f.nodes[in]; ok {
			node.loops = append(node.loops, out...)
		}
	}
	if f.FirstNode == "" {
		for _, t := range src {
			if !contains(dest, t) {
				f.FirstNode = t
			}
		}
	}
	if f.FirstNode != "" {
		f.firstNode = f.nodes[f.FirstNode]
	} else {
		if len(f.nodes) > 0 {
			for _, n := range f.nodes {
				f.firstNode = n
				break
			}
		}
	}
	f.prepared = true
}

func (f *Flow) SetupServer() error {
	if f.config.NoService || f.Mode == Sync {
		return nil
	}
	f.prepareNodes()
	mux := NewServeMux()
	for node, handler := range f.nodes {
		f.server.AddQueue(node, 1)
		f.rdb.Client().SAdd(context.Background(), base.AllQueues(), node)
		result := mux.Handle(node, handler)
		if result.Error != nil {
			return result.Error
		}
	}

	key := "cron:1:" + f.ID
	if f.config.CronReportHandler == nil {
		mux.Handle(key, &CronReportHandler{flow: f})
	} else {
		mux.Handle(key, f.config.CronReportHandler)
	}
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
		_ = f.scheduler.Start()
	}

	if f.server.state.value != srvStateActive {
		return f.server.Start()
	}
	return nil
}

func (f *Flow) Shutdown() {
	if f.server == nil {
		return
	}
	f.scheduler.Shutdown()
	f.server.Shutdown()
}

func (f *Flow) SetPayload(_ Payload) {

}

func (f *Flow) SetKey(key string) {
	f.Key = key
}

func (f *Flow) Send(ctx context.Context, data []byte) Result {
	if f.config.Mode == Async {
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
	return f.ProcessTask(ctx, NewTask(f.FirstNode, data, FlowID(f.ID)))
}

func (f *Flow) SendAsync(data []byte) (*TaskInfo, error) {
	return SendAsync(f.config.RedisServer, f.ID, f.FirstNode, data)
}

func SendAsync(redisAddress, flowID string, queue string, data []byte) (*TaskInfo, error) {
	task := NewTask(queue, data, FlowID(flowID))
	client := NewClient(RedisClientOpt{Addr: redisAddress})
	defer func() {
		_ = client.Close()
	}()
	var ops []Option
	ops = append(ops, Queue(queue), FlowID(flowID))
	return client.Enqueue(task, ops...)
}

func (f *Flow) Enqueue(ctx context.Context, queueName string, broker base.Broker, flowID string, payload []byte, result *Result) {
	task := NewTask(queueName, payload, FlowID(flowID), Queue(queueName))
	_, err := EnqueueContext(broker, ctx, task, FlowID(flowID), Queue(queueName))
	if err != nil {
		result.Error = err
	}
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
					if _, ok := s[f.config.idKey]; !ok {
						s[f.config.idKey] = id
					}
					if _, ok := s[f.config.flowIDKey]; !ok {
						s[f.config.flowIDKey] = task.FlowID
					}
					if _, ok := s[f.config.statusKey]; !ok {
						s[f.config.statusKey] = "pending"
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
				if ft, ok := f.nodes[task.Type()]; ok {
					for _, v := range ft.loops {
						f.Enqueue(ctx, v, task.ResultWriter().Broker(), task.FlowID, payload, &result)
						if result.Error != nil {
							return result
						}
					}
				}
			}
		}
		if h.GetType() == "condition" {
			if ft, ok := f.Branches[task.Type()]; ok && result.Status != "" {
				if c, o := ft[result.Status]; o {
					f.Enqueue(ctx, c, task.ResultWriter().Broker(), task.FlowID, result.Data, &result)
					if result.Error != nil {
						return result
					}
				}
			} else if result.Status == "branches" {
				var r map[string]any
				err := json.Unmarshal(result.Data, &r)
				if err != nil {
					result.Error = err
					return result
				}
				for handler, data := range r {
					bt, _ := json.Marshal(data)
					f.Enqueue(ctx, handler, task.ResultWriter().Broker(), task.FlowID, bt, &result)
					if result.Error != nil {
						return result
					}
				}
			}
		}
		if ft, ok := f.nodes[task.Type()]; ok {
			for _, v := range ft.edges {
				f.Enqueue(ctx, v, task.ResultWriter().Broker(), task.FlowID, result.Data, &result)
				if result.Error != nil {
					return result
				}
			}
		}
		return result
	})
}
