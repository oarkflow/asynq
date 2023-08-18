package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/oarkflow/asynq"
)

const redisAddrWorker = "127.0.0.1:6379"

func main() {
	// send(asynq.Sync)
	sendA(asynq.Async)
}

var d = map[string]interface{}{
	"data_branch": map[string]string{
		"cpt":   "send:sms",
		"names": "notification",
	},
	"names": []string{"John", "Jane", "abc"},
	"cpt": []map[string]any{
		{
			"code":              "001",
			"encounter_uid":     "1",
			"billing_provider":  "Test provider",
			"resident_provider": "Test Resident Provider",
		},
		{
			"code":              "OBS01",
			"encounter_uid":     "1",
			"billing_provider":  "Test provider",
			"resident_provider": "Test Resident Provider",
		},
		{
			"code":              "SU002",
			"billing_provider":  "Test provider",
			"resident_provider": "Test Resident Provider",
		},
	},
}

func send(mode asynq.Mode) {
	f := asynq.NewFlow(asynq.Config{Mode: mode, RedisServer: redisAddrWorker})
	f.FirstNode = "get:input"
	f.
		AddHandler("get:input", &GetData{Operation{Type: "input"}}).
		AddHandler("send:sms", &SendSms{Operation{Type: "process"}}).
		AddHandler("notification", &InAppNotification{Operation{Type: "process"}}).
		AddHandler("data-branch", &DataBranchHandler{Operation{Type: "condition"}}).
		AddEdge("get:input", "data-branch")
	bt, _ := json.Marshal(d)
	data := f.Send(context.Background(), bt)
	fmt.Println(string(data.Data), data.Error)
}

func sendA(mode asynq.Mode) {
	f := asynq.NewFlow(asynq.Config{Mode: mode, RedisServer: redisAddrWorker})
	f.FirstNode = "get:input"
	f.AddHandler("email:deliver", &EmailDelivery{Operation{Type: "process"}}).
		AddHandler("prepare:email", &PrepareEmail{Operation{Type: "process"}}).
		AddHandler("get:input", &GetData{Operation{Type: "input"}}).
		AddHandler("loop", &Loop{Operation{Type: "loop"}}).
		AddHandler("condition", &Condition{Operation{Type: "condition"}}).
		AddHandler("store:data", &StoreData{Operation{Type: "process"}}).
		AddHandler("send:sms", &SendSms{Operation{Type: "process"}}).
		AddHandler("notification", &InAppNotification{Operation{Type: "process"}}).
		AddHandler("data-branch", &DataBranchHandler{Operation{Type: "condition"}}).
		AddBranch("data-branch", map[string]string{}).
		AddBranch("condition", map[string]string{
			"pass": "email:deliver",
			"fail": "store:data",
		}).
		AddEdge("get:input", "loop").
		AddLoop("loop", "prepare:email").
		AddEdge("prepare:email", "condition").
		AddEdge("store:data", "send:sms").
		AddEdge("store:data", "notification")
	data := []map[string]any{
		{
			"phone": "+123456789",
			"email": "abc.xyz@gmail.com",
		},
		{
			"phone": "+98765412",
			"email": "xyz.abc@gmail.com",
		},
	}
	bt, _ := json.Marshal(data)
	f.Send(context.Background(), bt)
	if f.Mode == asynq.Async {
		f.SetupServer()
		go func() {
			if err := f.Run(); err != nil {
				log.Fatalf("could not run server: %v", err)
			}
		}()
		time.Sleep(10 * time.Second)
		f.Shutdown()
	}
}
