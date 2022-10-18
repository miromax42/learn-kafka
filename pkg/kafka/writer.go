package kafka

import (
	"context"
	"fmt"

	kf "github.com/segmentio/kafka-go"

	"github.com/miromax42/learn-kafka/pkg/common"
)

type Writer struct {
	name string
	kr   *kf.Writer
}

func NewWriter(name string, cfg Config) *Writer {
	writer := &kf.Writer{
		Addr:  kf.TCP(cfg.Broker),
		Topic: cfg.Topic,
	}

	return &Writer{name, writer}
}

func (w *Writer) Write(ctx context.Context, inputStream <-chan string) <-chan error {
	errStream := make(chan error)

	go func() {
		defer close(errStream)

		for v := range common.OrDone(ctx.Done(), inputStream) {
			select {
			case <-ctx.Done():
				return
			case errStream <- w.kr.WriteMessages(ctx, kf.Message{Value: []byte(v)}):
				fmt.Printf("write:%s:%s\n", w.name, v)
			}
		}
	}()

	return errStream
}
