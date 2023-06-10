package asynq

import (
	"context"
	json "github.com/bytedance/sonic"
	"github.com/oarkflow/errors"
	"github.com/oarkflow/pkg/dipper"
	"github.com/oarkflow/pkg/evaluate"
	"github.com/oarkflow/pkg/str"
	"strings"
)

type Provider struct {
	Mapping      map[string]any `json:"mapping"`
	Defaults     map[string]any `json:"defaults"`
	ProviderType string         `json:"provider_type"`
	Database     string         `json:"database"`
	Source       string         `json:"source"`
	Query        string         `json:"query"`
}

type Payload struct {
	Data            map[string]any    `json:"data"`
	Mapping         map[string]string `json:"mapping"`
	GeneratedFields []string          `json:"generated_fields"`
	Providers       []Provider        `json:"providers"`
}

type Operation struct {
	Type            string   `json:"type"`
	Key             string   `json:"key"`
	RequiredFields  []string `json:"required_fields"`
	OptionalFields  []string `json:"optional_fields"`
	GeneratedFields []string `json:"generated_fields"`
	Payload         Payload
}

func (e *Operation) ProcessTask(ctx context.Context, task *Task) Result {
	return Result{Data: task.Payload(), Ctx: ctx}
}

func (e *Operation) SetPayload(payload Payload) {
	e.Payload = payload
	e.GeneratedFields = append(e.GeneratedFields, payload.GeneratedFields...)
}

func (e *Operation) GetType() string {
	return e.Type
}

func (e *Operation) GetKey() string {
	return e.Key
}

func (e *Operation) ValidateFields(payload []byte) (map[string]any, error) {
	var keys []string
	var data map[string]any
	err := json.Unmarshal(payload, &data)
	if err != nil {
		return nil, err
	}
	for k, v := range e.Payload.Mapping {
		_, val := e.GetVal(v, data)
		if val != nil {
			keys = append(keys, k)
		}
	}
	for k := range e.Payload.Data {
		keys = append(keys, k)
	}
	for _, k := range e.RequiredFields {
		if !str.Contains(keys, k) {
			return nil, errors.New("Required field doesn't exist")
		}
	}
	return data, nil
}

func (e *Operation) getVal(v string, data map[string]any) (key string, val any) {
	if strings.Contains(v, "*_") {
		fieldSuffix := strings.ReplaceAll(v, "*", "")
		for k, vt := range data {
			if strings.HasSuffix(k, fieldSuffix) {
				val = vt
				key = k
			}
		}
	} else {
		vd := dipper.Get(data, v)
		if dipper.Error(vd) == nil {
			val = vd
			key = v
		}
	}
	return
}

func (e *Operation) GetVal(v string, data map[string]any) (key string, val any) {
	vParts := strings.Split(v, ".")
	switch vParts[0] {
	case "body":
		v := vParts[1]
		key, val = e.getVal(v, data)
	case "param":
		v := vParts[1]
		param := data["request_param"].(map[string]any)
		key, val = e.getVal(v, param)
	case "query":
		v := vParts[1]
		query := data["request_query"].(map[string]any)
		key, val = e.getVal(v, query)
	case "eval":
		v := vParts[1]
		p, _ := evaluate.Parse(v, true)
		pr := evaluate.NewEvalParams(data)
		val, err := p.Eval(pr)
		if err != nil {
			return "", nil
		} else {
			return v, val
		}
	default:
		key, val = e.getVal(v, data)
	}
	return
}

func (e *Operation) GetExtraParams(ctx context.Context) map[string]any {
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

func (e *Operation) PrepareData(ctx context.Context, data any) {
	extraParams := e.GetExtraParams(ctx)
	switch data := data.(type) {
	case map[string]any:
		for key, val := range extraParams {
			data[key] = val
		}
	case []map[string]any:
		for _, d := range data {
			for key, val := range extraParams {
				d[key] = val
			}
		}
	case []any:
		for _, d := range data {
			switch d := d.(type) {
			case map[string]any:
				for key, val := range extraParams {
					d[key] = val
				}
			}
		}
	}
}
