package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	CommitStrategyAuto int = iota
	CommitStrategyPerFetch
	CommitStrategyNoCommit
)

type Consumer struct {
	franzClient        *kgo.Client
	consumerTopic      string
	seeds              []string
	groupName          string
	autoCommitInterval time.Duration
	commitStrategy     int
}

func NewConsumer(options ...func(*Consumer)) (*Consumer, error) {
	consumer := &Consumer{}
	for _, opt := range options {
		opt(consumer)
	}

	kOptions := []kgo.Opt{
		kgo.ConsumeTopics(consumer.consumerTopic),
		kgo.ConsumerGroup(consumer.groupName),
		kgo.SeedBrokers(consumer.seeds...),
	}
	switch consumer.commitStrategy {
	case CommitStrategyAuto:
		kOptions = append(kOptions, kgo.AutoCommitInterval(consumer.autoCommitInterval))
	case CommitStrategyPerFetch, CommitStrategyNoCommit:
		kOptions = append(kOptions, kgo.DisableAutoCommit())
	}
	client, err := kgo.NewClient(kOptions...)
	if err != nil {
		return nil, err
	}

	consumer.franzClient = client

	return consumer, nil
}

func WithCommitStrategy(strategy int) func(*Consumer) {
	return func(c *Consumer) {
		c.commitStrategy = strategy
	}
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

func (c *Consumer) Consume(ctx context.Context, callback func([]byte)) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			fetches := c.franzClient.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				return errors.New(fmt.Sprint(errs))
			}

			// process each record
			fetches.EachRecord(func(r *kgo.Record) {
				callback(r.Value)
			})

			// commit offset basis the strategy
			switch c.commitStrategy {
			case CommitStrategyPerFetch:
				c.franzClient.CommitUncommittedOffsets(ctx)
			case CommitStrategyAuto, CommitStrategyNoCommit:
			}
		}
	}
}
