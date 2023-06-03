package main

import (
	"context"
	"encoding/json"
	"fmt"

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
	if data["email"].(string) == "abc.xyz@gmail.com" {
		fmt.Println("Checking...", data, "Pass...")
		return asynq.Result{Data: task.Payload(), Status: "pass", Ctx: ctx}
	}
	fmt.Println("Checking...", data, "Fail...")
	return asynq.Result{Data: task.Payload(), Status: "fail", Ctx: ctx}
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
	data, _, _ := task.AsMap()
	fmt.Println("Sending Email...", data)
	return asynq.Result{Data: task.Payload(), Ctx: ctx}
}

type StoreData struct {
	Operation
}

func (e *StoreData) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	data, _, _ := task.AsMap()
	fmt.Println("Storing Data...", data)
	return asynq.Result{Data: task.Payload(), Ctx: ctx}
}
