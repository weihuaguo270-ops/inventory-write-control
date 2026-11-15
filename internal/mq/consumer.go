package mq

import (
	"context"
	"encoding/json"
	"log"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/weihuaguo270-ops/inventory-write-control/internal/httpapi"
)

type Consumer struct {
	c rocketmq.PushConsumer
}

func StartConsumer(nameSrv, group, topic string, dispatch func(context.Context, httpapi.EventBody) error) (*Consumer, error) {
	c, err := rocketmq.NewPushConsumer(
		consumer.WithNameServer([]string{nameSrv}),
		consumer.WithGroupName(group),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromLastOffset),
		consumer.WithConsumerModel(consumer.Clustering),
	)
	if err != nil {
		return nil, err
	}
	err = c.Subscribe(topic, consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			var body httpapi.EventBody
			if err := json.Unmarshal(msg.Body, &body); err != nil {
				log.Printf("mq: bad payload: %v", err)
				continue
			}
			if err := dispatch(ctx, body); err != nil {
				log.Printf("mq: dispatch failed event_id=%s type=%s: %v", body.EventID, body.Type, err)
				return consumer.ConsumeRetryLater, err
			}
			log.Printf("mq: ok event_id=%s type=%s", body.EventID, body.Type)
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		return nil, err
	}
	if err := c.Start(); err != nil {
		return nil, err
	}
	return &Consumer{c: c}, nil
}

func (c *Consumer) Shutdown() error {
	if c == nil || c.c == nil {
		return nil
	}
	return c.c.Shutdown()
}
