package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/oarkflow/asynq/errors"

	"github.com/oarkflow/pkg/dipper"

	"github.com/oarkflow/asynq"
)

type Operation struct {
	Type string `json:"type"`
	Key  string `json:"key"`
}

func (e *Operation) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	return asynq.Result{Data: task.Payload(), Ctx: ctx}
}

func (e *Operation) GetType() string {
	return e.Type
}

func (e *Operation) GetKey() string {
	return e.Key
}

type GetData struct {
	Operation
}

func (e *GetData) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	return asynq.Result{Error: errors.New("Error 2")}
	fmt.Println("Getting Input", string(task.Payload()))
	return asynq.Result{Data: task.Payload(), Ctx: ctx}
}

type Loop struct {
	Operation
}

func (e *Loop) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	cnt := context.WithValue(ctx, "extra_params", map[string]any{"iphone": true})
	return asynq.Result{Data: task.Payload(), Ctx: cnt}
}

type Condition struct {
	Operation
}

func (e *Condition) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var data map[string]any
	json.Unmarshal(task.Payload(), &data)
	switch email := data["email"].(type) {
	case string:
		if email == "abc.xyz@gmail.com" {
			fmt.Println("Checking...", data, "Pass...")
			return asynq.Result{Data: task.Payload(), Status: "pass", Ctx: ctx}
		}
		fmt.Println("Checking...", data, "Fail...")
		return asynq.Result{Data: task.Payload(), Status: "fail", Ctx: ctx}
	}
	return asynq.Result{Data: task.Payload(), Status: "", Ctx: ctx}
}

type PrepareEmail struct {
	Operation
}

func (e *PrepareEmail) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var data map[string]any
	json.Unmarshal(task.Payload(), &data)
	data["email_valid"] = true
	d, _ := json.Marshal(data)
	fmt.Println("Preparing...", string(d))
	return asynq.Result{Data: d, Ctx: ctx}
}

type EmailDelivery struct {
	Operation
}

func (e *EmailDelivery) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var data map[string]any
	json.Unmarshal(task.Payload(), &data)
	fmt.Println("Sending Email...", data)
	return asynq.Result{Data: task.Payload(), Ctx: ctx}
}

type SendSms struct {
	Operation
}

func (e *SendSms) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	return asynq.Result{Error: errors.New("Somethign wrong"), Ctx: ctx}
	fmt.Println(ctx.Value("extra_params"))
	var data map[string]any
	json.Unmarshal(task.Payload(), &data)
	fmt.Println("Sending Sms...", data)
	return asynq.Result{Error: errors.New("Somethign wrong"), Ctx: ctx}
}

type StoreData struct {
	Operation
}

func (e *StoreData) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var data map[string]any
	json.Unmarshal(task.Payload(), &data)
	fmt.Println("Storing Data...", data)
	return asynq.Result{Data: task.Payload(), Ctx: ctx}
}

type InAppNotification struct {
	Operation
}

func (e *InAppNotification) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	fmt.Println(ctx.Value("extra_params"))
	var data map[string]any
	json.Unmarshal(task.Payload(), &data)
	fmt.Println("In App notification...", data)
	return asynq.Result{Data: task.Payload(), Ctx: ctx}
}

type DataBranchHandler struct{ Operation }

func (v *DataBranchHandler) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	ctx = context.WithValue(ctx, "extra_params", map[string]any{"iphone": true})
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
	switch branches := row["data_branch"].(type) {
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
