package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Consumer struct {
	franzClient        *kgo.Client
	consumerTopic      string
	seeds              []string
	groupName          string
	autoCommitInterval time.Duration
}

func NewConsumer(options ...func(*Consumer)) (*Consumer, error) {
	consumer := &Consumer{}
	for _, opt := range options {
		opt(consumer)
	}

	client, err := kgo.NewClient(
		kgo.ConsumeTopics(consumer.consumerTopic),
		kgo.ConsumerGroup(consumer.groupName),
		kgo.SeedBrokers(consumer.seeds...),
		kgo.AutoCommitInterval(consumer.autoCommitInterval),
	)
	if err != nil {
		return nil, err
	}

	consumer.franzClient = client

	return consumer, nil
}

func WithTopic(topic string) func(*Consumer) {
	return func(c *Consumer) {
		c.consumerTopic = topic
	}
}

func WithSeedBrokers(hosts ...string) func(*Consumer) {
	return func(c *Consumer) {
		c.seeds = hosts
	}
}

func WithConsumerGroup(groupName string) func(*Consumer) {
	return func(c *Consumer) {
		c.groupName = groupName
	}
}

func WithCommitInterval(interval time.Duration) func(*Consumer) {
	return func(c *Consumer) {
		c.autoCommitInterval = interval
	}
}

func (c *Consumer) Close() {
	c.franzClient.Close()
}

func (c *Consumer) Consume(ctx context.Context, callback func([]byte) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			fetches := c.franzClient.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				return errors.New(fmt.Sprint(errs))
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()

				err := callback(record.Value)
				if err != nil {
					fmt.Println("error in callback", err)
				}
			}
		}
	}
}
