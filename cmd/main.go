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
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		log.Println("Stoping!")
		cancel()
		time.Sleep(10 * time.Second)
	}()

	err := kafka_test.New(ctx)
	if err != nil {
		log.Panic("failed to start kafka:", err)
	}

	workCtx, workCancel := context.WithTimeout(ctx, time.Second*10)

	var i int
	workGen := common.RepeatFn(workCtx.Done(), func() string {
		time.Sleep(time.Second / 2)
		i++

		return "msg" + strconv.Itoa(i)
	})

	reader1 := kafka.NewReader("r1", "test")
	reader2 := kafka.NewReader("r2", "test")
	reader3 := kafka.NewReader("r3", "another_group")
	writer := kafka.NewWriter("w1")

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
}
