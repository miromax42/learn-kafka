package kafka

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	kf "github.com/segmentio/kafka-go"

	"github.com/miromax42/learn-kafka/pkg/common"
)

type Reader struct {
	name, group string
	kr          *kf.Reader
}

type Message struct {
	kf.Message
	Err error
}

func NewReader(name, group string, cfg Config) *Reader {
	reader := kf.NewReader(
		kf.ReaderConfig{
			Brokers: []string{cfg.Broker},
			Topic:   cfg.Topic,
			GroupID: group,
		})

	return &Reader{name, group, reader}
}

func (r *Reader) Read(ctx context.Context) <-chan Message {
	return r.commit(ctx, r.fetch(ctx))
}

func (r *Reader) fetch(ctx context.Context) <-chan Message {
	msgStream := make(chan Message)

	go func() {
		defer close(msgStream)

		for {
			kMsg, err := r.kr.FetchMessage(ctx)
			fmt.Printf("fetch:group=%s:%s:%s\n", r.group, r.name, string(kMsg.Value))

			msg := Message{
				Message: kMsg,
				Err:     err,
			}

			select {
			case <-ctx.Done():
				return
			case msgStream <- msg:
			}
		}
	}()

	return msgStream
}

func (r *Reader) commit(ctx context.Context, inputStream <-chan Message) <-chan Message {
	committedStream := make(chan Message)

	go func() {
		defer close(committedStream)

		for {
			for v := range common.OrDone(ctx.Done(), inputStream) {
				err := r.kr.CommitMessages(ctx, v.Message)
				v.Err = errors.CombineErrors(v.Err, err)
				fmt.Printf("commited:group=%s:%s:%s\n", r.group, r.name, string(v.Message.Value))
				select {
				case <-ctx.Done():
					return
				case committedStream <- v:
				}
			}
		}

	}()

	return committedStream
}
