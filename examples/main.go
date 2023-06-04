package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/oarkflow/asynq"
)

const redisAddrWorker = "127.0.0.1:6379"

func main() {
	send(asynq.Sync)
	// send(asynq.Async)
}
func send(mode asynq.Mode) {
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
	f.Send(bt)
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
