package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/miromax42/learn-kafka/pkg/common"
	"github.com/miromax42/learn-kafka/pkg/kafka"
	"github.com/miromax42/learn-kafka/pkg/kafka_test"
)

func main() {
	var err error
	workCtx, workCancel := context.WithTimeout(context.Background(), time.Second*10)

	var i int
	workGen := common.RepeatFn(workCtx.Done(), func() string {
		time.Sleep(time.Second / 2)
		i++

		return "msg" + strconv.Itoa(i)
	})

	cfg := kafka.Config{
		Broker: kafka_test.Broker(),
		Topic:  "test-topic3",
	}

	reader1 := kafka.NewReader("r1", "test", cfg)
	reader2 := kafka.NewReader("r2", "test", cfg)
	reader3 := kafka.NewReader("r3", "another_group", cfg)
	writer := kafka.NewWriter("w1", cfg)

	errStream := writer.Write(workCtx, workGen)
	msgStream1 := reader1.Read(workCtx)
	msgStream2 := reader2.Read(workCtx)
	msgStream3 := reader3.Read(workCtx)

	wg := sync.WaitGroup{}
	wg.Add(4)

	go func() {
		defer wg.Done()

		for err := range common.OrDone(workCtx.Done(), errStream) {
			if err != nil {
				log.Println("error:", err)
				workCancel()
			}
		}
	}()

	go func() {
		defer wg.Done()

		for msg := range common.OrDone(workCtx.Done(), msgStream1) {
			if msg.Err != nil {
				log.Println("error:", err)
				workCancel()
			}

			fmt.Println("get message: ", string(msg.Value))
		}
	}()

	go func() {
		defer wg.Done()

		for msg := range common.OrDone(workCtx.Done(), msgStream2) {
			if msg.Err != nil {
				log.Println("error:", err)
				workCancel()
			}

			fmt.Println("get message: ", string(msg.Value))
		}
	}()

	go func() {
		defer wg.Done()

		for msg := range common.OrDone(workCtx.Done(), msgStream3) {
			if msg.Err != nil {
				log.Println("error:", err)
				workCancel()
			}

			fmt.Println("finished: ", string(msg.Value))
		}
	}()

	wg.Wait()

	time.Sleep(1 * time.Second)
}
