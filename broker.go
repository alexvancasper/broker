package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ADD    MessageType = "add"
	DELETE MessageType = "delete"
	UPDATE MessageType = "update"
	PAUSE  MessageType = "pause"
	RESUME MessageType = "resume"
)

// publishTimeout ограничивает время одной публикации.
const publishTimeout = 5 * time.Second

type MessageType string

type MsgBroker struct {
	conn    *amqp.Connection
	channel *amqp.Channel

	// queue — последняя объявленная очередь. Оставлено ради обратной
	// совместимости с RegisterConsumer; для нового кода используйте queues.
	queue amqp.Queue

	mu sync.Mutex
	// queues — все объявленные очереди в порядке объявления.
	queues []amqp.Queue
	// durable хранит, с каким флагом durable очередь была объявлена.
	// Переобъявить существующую очередь с другим durable нельзя —
	// RabbitMQ отвечает PRECONDITION_FAILED и закрывает канал.
	durable map[string]bool
}

// MsgBrokerInit подключается к брокеру и объявляет неустойчивые (durable=false)
// очереди. Legacy-конструктор: новый код должен использовать MsgBrokerInitDurable.
func MsgBrokerInit(connStr string, queuesName []string) (*MsgBroker, error) {
	return msgBrokerInit(connStr, queuesName, false)
}

// MsgBrokerInitDurable подключается к брокеру и объявляет устойчивые очереди.
// Сообщения, опубликованные через PublishPersistent, переживают перезапуск брокера.
func MsgBrokerInitDurable(connStr string, queuesName []string) (*MsgBroker, error) {
	return msgBrokerInit(connStr, queuesName, true)
}

func msgBrokerInit(connStr string, queuesName []string, durable bool) (*MsgBroker, error) {
	msg := MsgBroker{durable: make(map[string]bool, len(queuesName))}
	if err := msg.connect(connStr); err != nil {
		return nil, err
	}
	if err := msg.createChannel(); err != nil {
		return nil, err
	}
	for _, queue := range queuesName {
		if err := msg.queueDeclare(queue, durable); err != nil {
			return nil, err
		}
	}
	return &msg, nil
}

func (m *MsgBroker) IsAlive() bool {
	return !m.conn.IsClosed()
}

// DeclareDurable объявляет устойчивую очередь. Повторный вызов для уже
// объявленного имени — no-op.
func (m *MsgBroker) DeclareDurable(queueName string) error {
	return m.queueDeclare(queueName, true)
}

// DeclareTransient объявляет неустойчивую (durable=false) очередь. Нужна, чтобы
// слушать legacy-очереди, созданные MsgBrokerInit: объявить их как durable нельзя —
// RabbitMQ ответит PRECONDITION_FAILED и закроет канал. Повторный вызов — no-op.
func (m *MsgBroker) DeclareTransient(queueName string) error {
	return m.queueDeclare(queueName, false)
}

// Queues возвращает имена объявленных очередей.
func (m *MsgBroker) Queues() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.queues))
	for _, q := range m.queues {
		names = append(names, q.Name)
	}
	return names
}

// RegisterConsumer подписывается на последнюю объявленную очередь с auto-ack.
//
// Deprecated: слушает только одну очередь и подтверждает сообщение до его
// обработки, поэтому падение обработчика теряет задание. Используйте
// RegisterConsumers.
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
		return nil, fmt.Errorf("RegisterConsumer error: %w", err)
	}
	return msg, nil
}

// RegisterConsumers подписывается на все объявленные очереди и сводит их в один
// канал. Auto-ack выключен: обработчик обязан вызвать Ack или Nack у каждого
// Delivery, иначе сообщение вернётся в очередь при обрыве соединения.
//
// Возвращённый канал закрывается, когда закрыты все исходные каналы.
func (m *MsgBroker) RegisterConsumers() (<-chan amqp.Delivery, error) {
	m.mu.Lock()
	queues := make([]amqp.Queue, len(m.queues))
	copy(queues, m.queues)
	m.mu.Unlock()

	if len(queues) == 0 {
		return nil, fmt.Errorf("RegisterConsumers error: no queues declared")
	}

	sources := make([]<-chan amqp.Delivery, 0, len(queues))
	for _, q := range queues {
		src, err := m.channel.Consume(
			q.Name, // queue
			"",     // consumer
			false,  // auto-ack выключен
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		if err != nil {
			return nil, fmt.Errorf("RegisterConsumers error on queue %q: %w", q.Name, err)
		}
		sources = append(sources, src)
	}

	if len(sources) == 1 {
		return sources[0], nil
	}

	out := make(chan amqp.Delivery)
	var wg sync.WaitGroup
	wg.Add(len(sources))
	for _, src := range sources {
		go func(c <-chan amqp.Delivery) {
			defer wg.Done()
			for d := range c {
				out <- d
			}
		}(src)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out, nil
}

// PublishMsg публикует сообщение без гарантии сохранности при перезапуске брокера.
//
// Deprecated: используйте PublishPersistent.
func (m *MsgBroker) PublishMsg(data []byte, msgType MessageType, queueName, dstSrv string) error {
	return m.publish(data, msgType, queueName, dstSrv, amqp.Transient)
}

// PublishPersistent публикует сообщение с DeliveryMode=Persistent. Если очередь
// ещё не объявлена этим соединением, она объявляется как durable — иначе публикация
// в несуществующую очередь молча пропадает (exchange по умолчанию, mandatory=false).
func (m *MsgBroker) PublishPersistent(data []byte, msgType MessageType, queueName, dstSrv string) error {
	if err := m.ensureDurable(queueName); err != nil {
		return err
	}
	return m.publish(data, msgType, queueName, dstSrv, amqp.Persistent)
}

func (m *MsgBroker) publish(data []byte, msgType MessageType, queueName, dstSrv string, mode uint8) error {
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()

	err := m.channel.PublishWithContext(ctx,
		"",        // exchange
		queueName, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Type:         string(msgType),
			Body:         data,
			AppId:        dstSrv,
			DeliveryMode: mode,
			Timestamp:    time.Now(),
		})
	if err != nil {
		return fmt.Errorf("failed to publish a %s message to %q: %w", msgType, queueName, err)
	}
	return nil
}

// ensureDurable объявляет очередь, если она ещё не объявлена этим соединением.
func (m *MsgBroker) ensureDurable(queueName string) error {
	m.mu.Lock()
	_, known := m.durable[queueName]
	m.mu.Unlock()
	if known {
		return nil
	}
	return m.queueDeclare(queueName, true)
}

func (m *MsgBroker) connect(connStr string) error {
	var err error
	m.conn, err = amqp.Dial(connStr) // "amqp://guest:guest@localhost:5672/"
	if err != nil {
		return fmt.Errorf("%s: %w", "Failed to connect to RabbitMQ", err)
	}
	return nil
}

func (m *MsgBroker) createChannel() error {
	var err error
	m.channel, err = m.conn.Channel()
	if err != nil {
		return fmt.Errorf("%s: %w", "Failed to open a channel", err)
	}
	return nil
}

func (m *MsgBroker) queueDeclare(queueName string, durable bool) error {
	m.mu.Lock()
	if m.durable == nil {
		m.durable = make(map[string]bool)
	}
	if was, known := m.durable[queueName]; known {
		m.mu.Unlock()
		if was != durable {
			return fmt.Errorf("queue %q already declared with durable=%t, cannot redeclare with durable=%t",
				queueName, was, durable)
		}
		return nil
	}
	m.mu.Unlock()

	q, err := m.channel.QueueDeclare(
		queueName, // name
		durable,   // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue %q: %w", queueName, err)
	}

	m.mu.Lock()
	m.queue = q
	m.queues = append(m.queues, q)
	m.durable[queueName] = durable
	m.mu.Unlock()
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
