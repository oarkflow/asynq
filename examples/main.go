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
	// sendA(asynq.Async)
	schedule()
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

func serve() {
	rdb := asynq.NewRDB(asynq.Config{RedisServer: "127.0.0.1:6379"})
	cfg := asynq.Config{RDB: rdb}
	srv := asynq.NewServer(cfg)
	mux := asynq.NewServeMux()
	srv.AddHandler(mux)
	srv.AddQueue("example", 1)
	srv.AddQueueHandler("example", handler)
	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
}

func handler(ctx context.Context, t *asynq.Task) asynq.Result {
	fmt.Println("Payload", time.Now(), string(t.Payload()))
	return asynq.Result{}
}

func schedule() {
	go serve()
	scheduler := asynq.NewScheduler(asynq.RedisClientOpt{Addr: "127.0.0.1:6379"}, nil)
	task := asynq.NewTask("example", nil)
	// You can use "@every <duration>" to specify the interval.
	entryID, err := scheduler.Register("@every 5s", task, asynq.Queue("example"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("registered an entry: %q\n", entryID)
	go func() {
		time.Sleep(10 * time.Second)
		scheduler.Unregister(entryID)
	}()
	if err := scheduler.Run(); err != nil {
		log.Fatal(err)
	}
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
