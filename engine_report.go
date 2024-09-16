package asynq

import (
	"context"
	"fmt"
	"strings"

	"github.com/oarkflow/json"

	"github.com/oarkflow/asynq/rdb"
)

type FlowError struct {
	Handler string `json:"handler"`
	Node    string `json:"node"`
	Type    string `json:"type"`
	error   error
}

func (f *FlowError) Error() string {
	var msg string
	var err []string
	if f.error != nil {
		msg = f.error.Error()
	}
	if f.Handler != "" {
		err = append(err, fmt.Sprintf("handler: %s", f.Handler))
	}
	if f.Node != "" {
		err = append(err, fmt.Sprintf("node: %s", f.Node))
	}
	if f.Type != "" {
		err = append(err, fmt.Sprintf("type: %s", f.Type))
	}
	errs := strings.Join(err, "; ")
	if !strings.Contains(msg, errs) {
		return msg + "\n" + strings.Join(err, "; ")
	}
	return msg
}

func NewFlowError(err error, flowID, node, nodeType string) error {
	return &FlowError{
		Handler: flowID,
		Node:    node,
		Type:    nodeType,
		error:   err,
	}
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

func (v *CronReportHandler) SetPayload(payload Payload) {

}

func (v *CronReportHandler) SetKey(key string) {
	v.Key = key
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
			data = mergeMap(src, data)
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

func contains[T comparable](s []T, v T) bool {
	for _, vv := range s {
		if vv == v {
			return true
		}
	}
	return false
}

func mergeMap(map1 map[string]any, map2 map[string]any) map[string]any {
	for k, m := range map2 {
		if _, ok := map1[k]; !ok {
			map1[k] = m
		}
	}
	return map1
}

func asMap(payload []byte) (data any, slice bool, err error) {
	var mp map[string]any
	err = json.Unmarshal(payload, &mp)
	if err != nil {
		var mps []map[string]any
		err = json.Unmarshal(payload, &mps)
		if err == nil {
			data = mps
			slice = true
		}
	} else {
		data = mp
	}
	return
}

func getExtraParams(ctx context.Context) map[string]any {
	extraParams := map[string]any{}
	ep := ctx.Value("extra_params")
	switch ep := ep.(type) {
	case map[string]any:
		extraParams = ep
	case string:
		json.Unmarshal([]byte(ep), &extraParams)
	}
	return extraParams
}
