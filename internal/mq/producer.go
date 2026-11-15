package mq

import (
	"context"
	"encoding/json"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/weihuaguo270-ops/inventory-write-control/internal/httpapi"
)

type Producer struct {
	p     rocketmq.Producer
	topic string
}

func StartProducer(nameSrv, group, topic string) (*Producer, error) {
	p, err := rocketmq.NewProducer(
		producer.WithNameServer([]string{nameSrv}),
		producer.WithGroupName(group),
		producer.WithRetry(2),
	)
	if err != nil {
		return nil, err
	}
	if err := p.Start(); err != nil {
		return nil, err
	}
	return &Producer{p: p, topic: topic}, nil
}

func (p *Producer) Publish(ctx context.Context, body httpapi.EventBody) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	msg := primitive.NewMessage(p.topic, raw)
	msg.WithKeys([]string{body.EventID})
	_, err = p.p.SendSync(ctx, msg)
	return err
}

func (p *Producer) Shutdown() error {
	if p == nil || p.p == nil {
		return nil
	}
	return p.p.Shutdown()
}
