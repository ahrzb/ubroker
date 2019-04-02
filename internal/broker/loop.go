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
		if c.published.Len() > 0 {
			c.runningLoop()
		} else if c.closed {
			c.closedLoop()
			return // Terminate the loop
		} else {
			c.handleCommand(<-c.mailBox)
		}
	}
}

func (c *core) runningLoop() {
	for {
		if c.published.Len() < 1 {
			return // Go back to the empty loop
		} else if c.closed {
			c.closedLoop()
			return // Go back to the empty loop(then terminate)
		} else {
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
		}
	}
}

func (c *core) closedLoop() {
	for command := range c.mailBox {
		c.handleCommandClosed(command)
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
		cmd.err <- ubroker.ErrClosed
	case publishCommand:
		cmd.err <- ubroker.ErrClosed
	case reQueueCommand:
		cmd.err <- ubroker.ErrClosed
	case acknowledgeCommand:
		cmd.err <- ubroker.ErrClosed
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
