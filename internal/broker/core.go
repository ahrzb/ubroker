package broker

import (
	"container/list"
	"context"
	"time"

	"github.com/arcana261/ubroker/pkg/ubroker"
)

// New creates a new instance of ubroker.Broker
// with given `ttl`. `ttl` determines time in which
// we requeue an unacknowledged/unrequeued message
// automatically.
func New(ttl time.Duration) ubroker.Broker {
	c := &core{
		ttl:        ttl,
		delivery:   make(chan ubroker.Delivery),
		mailBox:    make(chan interface{}),
		published:  list.New(),
		inProgress: make(map[int]ubroker.Delivery),
	}
	go c.loop()
	return c
}

type core struct {
	closed     bool
	ttl        time.Duration
	delivery   chan ubroker.Delivery
	mailBox    chan interface{}
	seqNO      int
	published  *list.List
	inProgress map[int]ubroker.Delivery
}

func (c *core) Delivery(ctx context.Context) (<-chan ubroker.Delivery, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		deliveryChannel := make(chan (<-chan ubroker.Delivery))
		err := make(chan error)
		c.mailBox <- obtainDeliveryCommand{
			deliveryChannel: deliveryChannel,
			err:             err,
		}
		return <-deliveryChannel, <-err
	}
}

func (c *core) Acknowledge(ctx context.Context, id int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		err := make(chan error)
		c.mailBox <- acknowledgeCommand{
			id:  id,
			err: err,
		}
		return <-err
	}
}

func (c *core) ReQueue(ctx context.Context, id int) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		err := make(chan error)
		c.mailBox <- reQueueCommand{
			id:  id,
			err: err,
		}
		return <-err
	}
}

func (c *core) Publish(ctx context.Context, message ubroker.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		err := make(chan error)
		c.mailBox <- publishCommand{
			message: message,
			err:     err,
		}
		return <-err
	}
}

func (c *core) Close() error {
	err := make(chan error)
	c.mailBox <- closeCommand{err: err}
	return <-err
}
