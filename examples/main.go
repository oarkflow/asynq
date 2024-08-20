package main

import (
	"context"
	"fmt"
	"github.com/oarkflow/json"
	"log"
	"time"

	"github.com/oarkflow/json"

	"github.com/oarkflow/asynq"
	"github.com/oarkflow/asynq/errors"
)

const redisAddrWorker = "127.0.0.1:6379"

func main() {
	// send(asynq.Async)
	sendA(asynq.Async)
	// schedule()
	/*go consumer1()
	go consumer2()
	sendTask()
	time.Sleep(10 * time.Second)*/
}

func handler(ctx context.Context, task *asynq.Task) asynq.Result {
	fmt.Println(task.Type(), string(task.Payload()))
	return asynq.Result{}
}

func handler2(ctx context.Context, task *asynq.Task) asynq.Result {
	fmt.Println("Handler 2", task.Type(), string(task.Payload()))
	return asynq.Result{Error: errors.New("Error 1")}
}

func sendTask() {
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddrWorker})
	task := asynq.NewTask("queue", []byte("Hello World"))
	_, err := client.Enqueue(task, asynq.Queue("queue"))
	if err != nil {
		panic(err)
	}
	task = asynq.NewTask("queue2", []byte("Hello World"))
	_, err = client.Enqueue(task, asynq.Queue("queue2"))
	if err != nil {
		panic(err)
	}
	time.Sleep(10 * time.Minute)

}

func consumer1() {
	rdb := asynq.NewRDB(asynq.Config{RedisServer: redisAddrWorker})
	srv1 := asynq.NewServer(asynq.Config{RDB: rdb})
	mux1 := asynq.NewServeMux()
	srv1.AddHandler(mux1)
	srv1.AddQueue("queue", 1)
	srv1.AddQueueHandler("queue", handler)
	if err := srv1.Start(); err != nil {
		panic(err)
	}

}

func consumer2() {
	rdb := asynq.NewRDB(asynq.Config{RedisServer: redisAddrWorker})

	srv1 := asynq.NewServer(asynq.Config{RDB: rdb})
	mux1 := asynq.NewServeMux()
	srv1.AddHandler(mux1)
	srv1.AddQueue("queue2", 1)
	srv1.AddQueueHandler("queue2", handler2)
	if err := srv1.Start(); err != nil {
		panic(err)
	}

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
		AddHandler("get:input", &GetData{asynq.Operation{Type: "input"}}).
		AddHandler("send:sms", &SendSms{asynq.Operation{Type: "process"}}).
		AddHandler("notification", &InAppNotification{asynq.Operation{Type: "process"}}).
		AddHandler("data-branch", &DataBranchHandler{asynq.Operation{Type: "condition"}}).
		AddEdge("get:input", "data-branch")
	bt, _ := json.Marshal(d)
	data := f.Send(context.Background(), bt)
	fmt.Println(string(data.Data), data.Error)
}

func sendA(mode asynq.Mode) {
	f := asynq.NewFlow(asynq.Config{Mode: mode, RedisServer: redisAddrWorker})
	f.FirstNode = "get:input"
	f.AddHandler("email:deliver", &EmailDelivery{asynq.Operation{Type: "process"}}).
		AddHandler("prepare:email", &PrepareEmail{asynq.Operation{Type: "process"}}).
		AddHandler("get:input", &GetData{asynq.Operation{Type: "input"}}).
		AddHandler("loop", &Loop{asynq.Operation{Type: "loop"}}).
		AddHandler("condition", &Condition{asynq.Operation{Type: "condition"}}).
		AddHandler("store:data", &StoreData{asynq.Operation{Type: "process"}}).
		AddHandler("send:sms", &SendSms{asynq.Operation{Type: "process"}}).
		AddHandler("notification", &InAppNotification{asynq.Operation{Type: "process"}}).
		AddHandler("data-branch", &DataBranchHandler{asynq.Operation{Type: "condition"}}).
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
