package main

import (
	json "github.com/bytedance/sonic"
	"log"
	"time"

	"github.com/oarkflow/asynq"
)

func main() {
	send(asynq.Sync)
}
func send(mode asynq.Mode) {
	cfg := asynq.Config{Mode: mode}
	flow := asynq.NewFlow(cfg)
	{
		err := flow.
			AddHandler("email:deliver", &EmailDelivery{Operation{Type: "process"}}).
			AddHandler("prepare:email", &PrepareEmail{Operation{Type: "process"}}).
			AddHandler("get:input", &GetData{Operation{Type: "input"}}).
			AddHandler("loop", &Loop{Operation{Type: "loop"}}).
			AddHandler("condition", &Condition{Operation{Type: "condition"}}).
			AddHandler("store:data", &StoreData{Operation{Type: "process"}}).
			AddBranch("condition", map[string]string{"pass": "email:deliver", "fail": "store:data"}).
			AddEdge("get:input", "loop").
			AddLoop("loop", "prepare:email").
			AddEdge("prepare:email", "condition").
			SetupServer()
		if err != nil {
			panic(err)
		}
	}

	bt, _ := json.Marshal(data)
	flow.Send(bt)
	if flow.Mode == asynq.Async {
		go func() {
			if err := flow.Run(); err != nil {
				log.Fatalf("could not run server: %v", err)
			}
		}()
		time.Sleep(10 * time.Second)
		flow.Shutdown()
	}
}

const redisAddrWorker = "127.0.0.1:6379"

var data = []map[string]any{
	{
		"phone": "+123456789",
		"email": "abc.xyz@gmail.com",
	},
	{
		"phone": "+98765412",
		"email": "xyz.abc@gmail.com",
	},
}
