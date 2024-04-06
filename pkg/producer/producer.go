package producer

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	franzClient  *kgo.Client
	produceTopic string
	seeds        []string
}

func NewProducer(options ...func(*Producer)) (*Producer, error) {
	producer := &Producer{}
	for _, opt := range options {
		opt(producer)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(producer.seeds...),
	)
	if err != nil {
		return nil, err
	}

	if err := client.Ping(context.Background()); err != nil {
		return nil, err
	}

	producer.franzClient = client

	return producer, nil
}

func WithTopic(topic string) func(*Producer) {
	return func(c *Producer) {
		c.produceTopic = topic
	}
}

func WithSeedBrokers(hosts ...string) func(*Producer) {
	return func(c *Producer) {
		c.seeds = hosts
	}
}

func (p *Producer) Produce(ctx context.Context, payload []byte) error {
	var (
		toReturnErr error
	)

	done := make(chan struct{})
	p.franzClient.Produce(
		ctx,
		&kgo.Record{
			Topic: p.produceTopic,
			Value: payload,
		},
		func(r *kgo.Record, err error) {
			if err != nil {
				toReturnErr = err
			}
			done <- struct{}{}
		},
	)

	<-done
	return toReturnErr
}

func (p *Producer) ProduceWithHeaders(ctx context.Context, payload []byte, headers map[string][]byte) error {
	var (
		toReturnErr error
		kHeaders    []kgo.RecordHeader
	)

	done := make(chan struct{})
	for key, value := range headers {
		kHeaders = append(kHeaders, kgo.RecordHeader{
			Key:   key,
			Value: value,
		})
	}

	p.franzClient.Produce(
		ctx,
		&kgo.Record{
			Topic:   p.produceTopic,
			Value:   payload,
			Headers: kHeaders,
		},
		func(r *kgo.Record, err error) {
			if err != nil {
				toReturnErr = err
			}
			done <- struct{}{}
		},
	)

	<-done
	return toReturnErr
}
