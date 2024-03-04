package broker

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ADD    MessageType = "add"
	DELETE MessageType = "delete"
)

type MessageType string

type MsgBroker struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

func MsgBrokerInit(connStr, queueName string) (*MsgBroker, error) {
	var msg MsgBroker
	var err error
	err = msg.connect(connStr)
	if err != nil {
		return nil, err
	}
	err = msg.createChannel()
	if err != nil {
		return nil, err
	}
	err = msg.queueDeclare(queueName)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (m *MsgBroker) IsAlive() bool {
	return !m.conn.IsClosed()
}

func (m *MsgBroker) RegisterConsumer() (<-chan amqp.Delivery, error) {
	msg, err := m.channel.Consume(
		m.queue.Name, // queue
		"",           // consumer
		true,         // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (m *MsgBroker) PublishMsg(data []byte, msgType MessageType) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := m.channel.PublishWithContext(ctx,
		"",           // exchange
		m.queue.Name, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Type:        string(msgType),
			Body:        data,
		})
	if err != nil {
		return fmt.Errorf("%s: %s", "Failed to publish a message", err)
	}
	log.Printf(" [x] Sent %s\n", data)
	return nil
}

func (m *MsgBroker) connect(connStr string) error {
	var err error
	m.conn, err = amqp.Dial(connStr) //"amqp://guest:guest@localhost:5672/"
	if err != nil {
		return fmt.Errorf("%s: %s", "Failed to connect to RabbitMQ", err)
	}
	return nil
}

func (m *MsgBroker) createChannel() error {
	var err error
	m.channel, err = m.conn.Channel()
	if err != nil {
		return fmt.Errorf("%s: %s", "Failed to open a channel", err)
	}
	return nil
}

func (m *MsgBroker) queueDeclare(queueName string) error {
	var err error
	m.queue, err = m.channel.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return fmt.Errorf("%s: %s", "Failed to declare queue", err)
	}
	return nil
}

func (m *MsgBroker) Close() {
	m.channelClose()
	m.connClose()
}

func (m *MsgBroker) connClose() {
	_ = m.conn.Close()
}
func (m *MsgBroker) channelClose() {
	_ = m.channel.Close()
}
