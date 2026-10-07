package event

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/94peter/mqtt"
	"github.com/94peter/mqtt/config"
	"github.com/94peter/mqtt/trans"
)

const (
	topicPrefix = "events/%s/%s/%s"
)

type MutiEventMsg interface {
	GetId() string
}

type MutiEventServ interface {
	Emit(service, model string, msg MutiEventMsg) error
	Register(service, model, msgId string, handler trans.Trans) error
	UnRegister(service, model, msgId string, handler trans.Trans)
	Run(context.Context)
}

type multiEventServ struct {
	mu           sync.RWMutex
	topicHandler map[string][]trans.Trans
	mqttServ     mqtt.MqttServer
}

func NewMultiService(cfg *config.Config) (MutiEventServ, error) {
	event := &multiEventServ{
		topicHandler: make(map[string][]trans.Trans),
	}
	cfg.AddTopics("events/#")
	mqttServ, err := mqtt.NewMqttServ(cfg, map[string]trans.Trans{
		"events/#": event,
	})
	if err != nil {
		return nil, err
	}

	event.mqttServ = mqttServ
	return event, nil
}

func (m *multiEventServ) Emit(service, model string, msg MutiEventMsg) error {
	topic := fmt.Sprintf(topicPrefix, service, model, msg.GetId())
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return m.mqttServ.Publish(ctx, topic, 0, data)
}

func (m *multiEventServ) Register(service, model, id string, handler trans.Trans) error {
	key := fmt.Sprintf(topicPrefix, service, model, id)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.topicHandler[key] = append(m.topicHandler[key], handler)
	return nil
}

func (m *multiEventServ) UnRegister(service, model, id string, handler trans.Trans) {
	key := fmt.Sprintf(topicPrefix, service, model, id)
	m.mu.Lock()
	defer m.mu.Unlock()
	handlers := m.topicHandler[key]
	remain := make([]trans.Trans, 0, len(handlers))
	for _, h := range handlers {
		if h != handler {
			remain = append(remain, h)
		}
	}
	if len(remain) == 0 {
		delete(m.topicHandler, key)
		return
	}
	m.topicHandler[key] = remain
}

func (m *multiEventServ) Send(topic string, msg []byte) error {
	// copy handlers so the lock is not held while a handler is running
	m.mu.RLock()
	handlers := append([]trans.Trans(nil), m.topicHandler[topic]...)
	m.mu.RUnlock()
	for _, handler := range handlers {
		if err := handler.Send(topic, msg); err != nil {
			return err
		}
	}
	return nil
}

func (m *multiEventServ) Close() {
	m.mqttServ.Close()
}

func (m *multiEventServ) Run(ctx context.Context) {
	m.mqttServ.Run(ctx)
}
