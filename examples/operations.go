package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/oarkflow/pkg/dipper"

	"github.com/oarkflow/asynq"
)

type Loop struct {
	asynq.Operation
}

func NewLoop(key string) *Loop {
	return &Loop{Operation: asynq.Operation{Type: "loop", Key: key}}
}

type StoreData struct {
	asynq.Operation
}

func NewStoreData(key string) *StoreData {
	return &StoreData{Operation: asynq.Operation{Key: key, Type: "process"}}
}

func (v *StoreData) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var data any
	err := json.Unmarshal(task.Payload(), &data)
	if err != nil {
		return asynq.Result{Error: err}
	}
	v.PrepareData(ctx, data)
	fmt.Println("STORE DATA...", data)
	return asynq.Result{Data: task.Payload(), Ctx: ctx}
}

type DataBranchHandler struct{ asynq.Operation }

func NewDataBranchHandler(key string, payload asynq.Payload) *DataBranchHandler {
	return &DataBranchHandler{Operation: asynq.Operation{
		Type:            "condition",
		Key:             key,
		GeneratedFields: payload.GeneratedFields,
		Payload:         payload,
	}}
}

func (v *DataBranchHandler) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var row map[string]any
	var result asynq.Result
	result.Data = task.Payload()
	err := json.Unmarshal(result.Data, &row)
	if err != nil {
		result.Error = err
		return result
	}
	fmt.Println("Data Branch...")
	b := make(map[string]any)
	switch branches := v.Payload.Data["data_branch"].(type) {
	case map[string]any:
		for field, handler := range branches {
			data := dipper.Get(row, field)
			switch data := data.(type) {
			case error, nil:
				break
			default:
				b[handler.(string)] = data
			}
		}
		break
	}
	br, err := json.Marshal(b)
	if err != nil {
		result.Error = err
		return result
	}
	result.Status = "branches"
	result.Data = br
	result.Ctx = ctx
	return result
}
