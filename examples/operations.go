package main

import (
	"context"
	"fmt"

	"github.com/oarkflow/json"

	"github.com/oarkflow/dipper"

	"github.com/oarkflow/asynq"
)

type GetData struct {
	asynq.Operation
}

func (e *GetData) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	fmt.Println("Getting Input", string(task.Payload()))
	return asynq.Result{Data: task.Payload(), Ctx: ctx}
}

type Loop struct {
	asynq.Operation
}

func (e *Loop) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	fmt.Println("Looping...", string(task.Payload()))
	cnt := context.WithValue(ctx, "extra_params", map[string]any{"iphone": true})
	return asynq.Result{Data: task.Payload(), Ctx: cnt}
}

type Condition struct {
	asynq.Operation
}

func (e *Condition) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload(), &data)
	if err != nil {
		panic(err)
	}
	switch email := data["email"].(type) {
	case string:
		if email == "abc.xyz@gmail.com" {
			fmt.Println("Checking...", data, "Pass...")
			return asynq.Result{Data: task.Payload(), Status: "pass", Ctx: ctx}
		}
		fmt.Println("Checking...", data, "Fail...")
		return asynq.Result{Data: task.Payload(), Status: "fail", Ctx: ctx}
	default:
		fmt.Println("Checking...", data, "Fail...")
	}
	return asynq.Result{Data: task.Payload(), Status: "", Ctx: ctx}
}

type PrepareEmail struct {
	asynq.Operation
}

func (e *PrepareEmail) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload(), &data)
	if err != nil {
		fmt.Println("Prepare Email")
		panic(err)
	}
	data["email_valid"] = true
	d, _ := json.Marshal(data)
	fmt.Println("Preparing Email...", string(d))
	return asynq.Result{Data: d, Ctx: ctx}
}

type EmailDelivery struct {
	asynq.Operation
}

func (e *EmailDelivery) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload(), &data)
	if err != nil {
		fmt.Println("Email Delivery")
		panic(err)
	}
	fmt.Println("Sending Email...", data)
	return asynq.Result{Data: task.Payload(), Ctx: ctx}
}

type SendSms struct {
	asynq.Operation
}

func (e *SendSms) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload(), &data)
	if err != nil {
		panic(err)
	}
	fmt.Println("Sending Sms...", data)
	return asynq.Result{Error: nil, Ctx: ctx}
}

type StoreData struct {
	asynq.Operation
}

func (e *StoreData) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload(), &data)
	if err != nil {
		panic(err)
	}
	fmt.Println("Storing Data...", data)
	return asynq.Result{Data: task.Payload(), Ctx: ctx}
}

type InAppNotification struct {
	asynq.Operation
}

func (e *InAppNotification) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	var data map[string]any
	err := json.Unmarshal(task.Payload(), &data)
	if err != nil {
		panic(err)
	}
	fmt.Println("In App notification...", data)
	return asynq.Result{Data: task.Payload(), Ctx: ctx}
}

type DataBranchHandler struct{ asynq.Operation }

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
			data, err := dipper.Get(row, field)
			if err != nil {
				break
			}
			b[handler.(string)] = data
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
