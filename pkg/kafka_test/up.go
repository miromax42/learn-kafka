package kafka_test

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/avast/retry-go"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"

	"github.com/miromax42/learn-kafka/config"
)

func New(ctx context.Context) error {
	initCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	compose := testcontainers.NewLocalDockerCompose(
		[]string{config.Root + "/pkg/kafka_test/test-docker-compose.yml"},
		strings.ToLower(uuid.New().String()),
	)

	exec := compose.WithCommand([]string{"up", "-d"}).Invoke()
	if exec.Error != nil {
		return exec.Error
	}

	go func() {
		<-ctx.Done()
		compose.Down()
		log.Println("Composed stoped")
	}()

	once := sync.Once{}
	return retry.Do(
		func() error {
			_, err := kafka.DialLeader(context.Background(), "tcp", Broker(), Topic(), 0)
			if err != nil {
				once.Do(func() {
					fmt.Println("Compose Waiting...")
				})
			} else {
				log.Println("Compose ready to accept connections")
			}
			return err
		},
		retry.Context(initCtx),
	)
}

func Topic() string {
	return "test-topic2"
}

func Broker() string {
	return "localhost:9092"
}
