package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/SiddhantAgarwal/go-kafka-lib/pkg/consumer"
)

const (
	consumerGroup = "test-group"
	consumeTopic  = "test-topic"
)

var (
	seeds = []string{"127.0.0.1:9092"}
)

func main() {
	ctx := context.Background()
	argsWithoutProg := os.Args[1:]
	consumerID := argsWithoutProg[0]

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan bool, 1)

	cctx, cancel := context.WithCancel(ctx)

	go func() {
		sig := <-sigs
		fmt.Println(sig)
		cancel()
		done <- true
	}()

	consumerInst, err := consumer.NewConsumer(
		consumer.WithSeedBrokers(seeds...),
		consumer.WithTopic(consumeTopic),
		consumer.WithConsumerGroup(consumerGroup),
		consumer.WithCommitStrategy(consumer.CommitStrategyPerFetch),
	)
	if err != nil {
		panic(err)
	}

	go func() {
		if err := consumerInst.Consume(cctx, func(b []byte) {
			fmt.Printf("consumer id: %s group: %s, message: %s\n", consumerID, consumerGroup, string(b))
		}); err != nil {
			fmt.Println(err)
			done <- true
		}
	}()

	<-done
	fmt.Println("consumer interrupted, exiting!")
	consumerInst.Close()
}
