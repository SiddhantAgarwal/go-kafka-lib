package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/SiddhantAgarwal/go-kafka-lib/pkg/producer"
)

const (
	topicName = "test-topic"
)

var (
	seeds = []string{"localhost:9092"}
)

func main() {
	ctx := context.Background()

	producerInst, err := producer.NewProducer(
		producer.WithSeedBrokers(seeds...),
		producer.WithTopic(topicName),
	)
	if err != nil {
		panic(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan bool, 1)

	go func() {
		sig := <-sigs
		fmt.Println(sig)
		cancel()
	}()

	go func() {
		for {
			select {
			case <-cctx.Done():
				done <- true
			default:
				producerInst.Produce(cctx, []byte(fmt.Sprintf("Message at %s", time.Now().Format(time.RFC3339))))
				time.Sleep(time.Millisecond * 400)
			}
		}
	}()

	<-done
	fmt.Println("producer exiting")
}
