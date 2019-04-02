package broker

import (
	"fmt"
	"time"

	"github.com/arcana261/ubroker/pkg/ubroker"
	"github.com/pkg/errors"
)

type obtainDeliveryCommand struct {
	deliveryChannel chan<- (<-chan ubroker.Delivery)
	err             chan<- error
}

type acknowledgeCommand struct {
	id  int
	err chan<- error
}

type reQueueCommand struct {
	id  int
	err chan<- error
}

type publishCommand struct {
	message ubroker.Message
	err     chan<- error
}

type closeCommand struct {
	err chan<- error
}

func (c *core) loop() {
	c.emptyLoop()
}

func (c *core) emptyLoop() {
	for {
		select {
		case command := <-c.mailBox:
			c.handleCommand(command)
		}

		if c.published.Len() > 0 {
			c.runningLoop()
		} else if c.closed {
			c.closedLoop()
		}
	}
}

func (c *core) runningLoop() {
	for {
		tip := c.published.Front()
		tipValue := tip.Value.(ubroker.Delivery)

		select {
		case command := <-c.mailBox:
			c.handleCommand(command)
		case c.delivery <- tipValue:
			c.inProgress[tipValue.ID] = tipValue
			c.published.Remove(tip)
			c.requeueAfterTTL(tipValue.ID)
		}

		if c.published.Len() < 1 {
			// Do the empty loop
			return
		} else if c.closed {
			c.closedLoop()
		}
	}
}

func (c *core) closedLoop() {
	for {
		select {
		case command := <-c.mailBox:
			c.handleCommandClosed(command)
		}
	}
}

func (c *core) handleCommand(command interface{}) {
	switch cmd := command.(type) {
	case closeCommand:
		cmd.err <- nil
		c.closed = true
		close(c.delivery)
	case obtainDeliveryCommand:
		cmd.deliveryChannel <- c.delivery
		cmd.err <- nil
	case publishCommand:
		c.published.PushBack(ubroker.Delivery{
			ID:      c.seqNO,
			Message: cmd.message,
		})
		c.seqNO++
		cmd.err <- nil
	case reQueueCommand:
		delivery, ok := c.inProgress[cmd.id]
		if !ok {
			cmd.err <- errors.Wrapf(ubroker.ErrInvalidID, "message with id %d not found", cmd.id)
			return
		}
		delete(c.inProgress, cmd.id)
		c.published.PushBack(ubroker.Delivery{
			ID:      c.seqNO,
			Message: delivery.Message,
		})
		cmd.err <- nil
		c.seqNO++
	case acknowledgeCommand:
		_, ok := c.inProgress[cmd.id]
		if !ok {
			cmd.err <- errors.Wrapf(ubroker.ErrInvalidID, "message with id %d not found", cmd.id)
			return
		}
		delete(c.inProgress, cmd.id)
		cmd.err <- nil
	default:
		panic(fmt.Sprintf("Unknown command in mailbox %v", cmd))
	}
}

func (c *core) handleCommandClosed(command interface{}) {
	switch cmd := command.(type) {
	case closeCommand:
		cmd.err <- nil
	case obtainDeliveryCommand:
		cmd.deliveryChannel <- nil
		cmd.err <- errors.Wrap(ubroker.ErrClosed, "Broker is shutting down")
	case publishCommand:
		cmd.err <- errors.Wrap(ubroker.ErrClosed, "Broker is shutting down")
	case reQueueCommand:
		cmd.err <- errors.Wrap(ubroker.ErrClosed, "Broker is shutting down")
	case acknowledgeCommand:
		cmd.err <- errors.Wrap(ubroker.ErrClosed, "Broker is shutting down")
	default:
		panic(fmt.Sprintf("Unknown command in mailbox %v", cmd))
	}
}

func (c *core) requeueAfterTTL(id int) {
	go func() {
		time.Sleep(c.ttl)
		err := make(chan error)
		c.mailBox <- reQueueCommand{id: id, err: err}
		<-err
	}()
}
