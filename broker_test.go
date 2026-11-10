package broker

import (
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Интеграционные тесты требуют живой RabbitMQ. Запуск:
//
//	docker run -d --rm --name broker-test -p 5673:5672 rabbitmq:3.10.7-management
//	BROKER_TEST_DSN="amqp://guest:guest@127.0.0.1:5673/" go test ./... -v
const dsnEnv = "BROKER_TEST_DSN"

func dsn(t *testing.T) string {
	t.Helper()
	v := os.Getenv(dsnEnv)
	if v == "" {
		t.Skipf("%s is not set, skipping integration test", dsnEnv)
	}
	return v
}

// uniqueQueue даёт имя, не пересекающееся с другими тестами и прогонами.
func uniqueQueue(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("test.%s.%d.%s", t.Name(), time.Now().UnixNano(), suffix)
}

func deleteQueues(t *testing.T, m *MsgBroker, names ...string) {
	t.Helper()
	for _, n := range names {
		if _, err := m.channel.QueueDelete(n, false, false, false); err != nil {
			t.Logf("cleanup: delete queue %q: %v", n, err)
		}
	}
}

func recv(t *testing.T, ch <-chan amqp.Delivery, timeout time.Duration) (amqp.Delivery, bool) {
	t.Helper()
	select {
	case d, ok := <-ch:
		return d, ok
	case <-time.After(timeout):
		return amqp.Delivery{}, false
	}
}

// TestRegisterConsumersFansInAllQueues — регрессия: RegisterConsumer читал только
// последнюю объявленную очередь, поэтому агент, слушающий две очереди, терял одну.
func TestRegisterConsumersFansInAllQueues(t *testing.T) {
	qA := uniqueQueue(t, "a")
	qB := uniqueQueue(t, "b")

	m, err := MsgBrokerInitDurable(dsn(t), []string{qA, qB})
	if err != nil {
		t.Fatalf("MsgBrokerInitDurable: %v", err)
	}
	defer m.Close()
	defer deleteQueues(t, m, qA, qB)

	deliveries, err := m.RegisterConsumers()
	if err != nil {
		t.Fatalf("RegisterConsumers: %v", err)
	}

	if err := m.PublishPersistent([]byte(`{"n":1}`), ADD, qA, "srvA"); err != nil {
		t.Fatalf("publish to %s: %v", qA, err)
	}
	if err := m.PublishPersistent([]byte(`{"n":2}`), DELETE, qB, "srvB"); err != nil {
		t.Fatalf("publish to %s: %v", qB, err)
	}

	var bodies, types []string
	for i := 0; i < 2; i++ {
		d, ok := recv(t, deliveries, 5*time.Second)
		if !ok {
			t.Fatalf("timed out waiting for message %d of 2 — fan-in dropped a queue", i+1)
		}
		bodies = append(bodies, string(d.Body))
		types = append(types, d.Type)
		if err := d.Ack(false); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	sort.Strings(bodies)
	sort.Strings(types)

	if bodies[0] != `{"n":1}` || bodies[1] != `{"n":2}` {
		t.Errorf("got bodies %v, want both queues represented", bodies)
	}
	if types[0] != string(ADD) || types[1] != string(DELETE) {
		t.Errorf("got types %v, want [add delete]", types)
	}
}

// TestPublishPersistentSurvivesConsumerRestart — сообщение не должно пропадать,
// если потребителя в момент публикации нет (auto-ack терял такие задания).
func TestPublishPersistentSurvivesConsumerRestart(t *testing.T) {
	q := uniqueQueue(t, "q")

	producer, err := MsgBrokerInitDurable(dsn(t), []string{q})
	if err != nil {
		t.Fatalf("producer init: %v", err)
	}
	defer producer.Close()
	defer deleteQueues(t, producer, q)

	if err := producer.PublishPersistent([]byte(`{"tunnel":"bb3b099"}`), UPDATE, q, "srv"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Потребитель приходит уже после публикации.
	consumer, err := MsgBrokerInitDurable(dsn(t), []string{q})
	if err != nil {
		t.Fatalf("consumer init: %v", err)
	}
	defer consumer.Close()

	deliveries, err := consumer.RegisterConsumers()
	if err != nil {
		t.Fatalf("RegisterConsumers: %v", err)
	}
	d, ok := recv(t, deliveries, 5*time.Second)
	if !ok {
		t.Fatal("message published before the consumer started was lost")
	}
	if string(d.Body) != `{"tunnel":"bb3b099"}` {
		t.Errorf("got body %q", d.Body)
	}
	if d.DeliveryMode != amqp.Persistent {
		t.Errorf("DeliveryMode = %d, want %d (persistent)", d.DeliveryMode, amqp.Persistent)
	}
	if err := d.Ack(false); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

// TestNackRequeuesJob — необработанное задание должно вернуться в очередь,
// а не исчезнуть, как это было с auto-ack.
func TestNackRequeuesJob(t *testing.T) {
	q := uniqueQueue(t, "q")

	m, err := MsgBrokerInitDurable(dsn(t), []string{q})
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	defer m.Close()
	defer deleteQueues(t, m, q)

	deliveries, err := m.RegisterConsumers()
	if err != nil {
		t.Fatalf("RegisterConsumers: %v", err)
	}
	if err := m.PublishPersistent([]byte(`{"n":1}`), ADD, q, "srv"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	first, ok := recv(t, deliveries, 5*time.Second)
	if !ok {
		t.Fatal("timed out waiting for the first delivery")
	}
	if err := first.Nack(false, true); err != nil {
		t.Fatalf("nack: %v", err)
	}

	second, ok := recv(t, deliveries, 5*time.Second)
	if !ok {
		t.Fatal("nacked message was not redelivered")
	}
	if !second.Redelivered {
		t.Error("Redelivered flag is not set on the second delivery")
	}
	if err := second.Ack(false); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

// TestPublishPersistentDeclaresUnknownQueue — публикация в очередь, о которой
// соединение ещё не знает, не должна молча пропадать (exchange по умолчанию,
// mandatory=false).
func TestPublishPersistentDeclaresUnknownQueue(t *testing.T) {
	declared := uniqueQueue(t, "declared")
	unknown := uniqueQueue(t, "unknown")

	m, err := MsgBrokerInitDurable(dsn(t), []string{declared})
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	defer m.Close()
	defer deleteQueues(t, m, declared, unknown)

	if err := m.PublishPersistent([]byte(`{"n":1}`), ADD, unknown, "srv"); err != nil {
		t.Fatalf("publish to an undeclared queue: %v", err)
	}

	names := m.Queues()
	found := false
	for _, n := range names {
		if n == unknown {
			found = true
		}
	}
	if !found {
		t.Fatalf("queue %q was not declared on publish; Queues() = %v", unknown, names)
	}

	deliveries, err := m.RegisterConsumers()
	if err != nil {
		t.Fatalf("RegisterConsumers: %v", err)
	}
	d, ok := recv(t, deliveries, 5*time.Second)
	if !ok {
		t.Fatal("message published to an undeclared queue was lost")
	}
	if err := d.Ack(false); err != nil {
		t.Fatalf("ack: %v", err)
	}
}

// TestRedeclareWithDifferentDurabilityIsRejected — переобъявление очереди с другим
// durable роняет канал в RabbitMQ (PRECONDITION_FAILED). Ловим это до похода в сеть.
func TestRedeclareWithDifferentDurabilityIsRejected(t *testing.T) {
	q := uniqueQueue(t, "q")

	m, err := MsgBrokerInit(dsn(t), []string{q}) // durable=false
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	defer m.Close()
	defer deleteQueues(t, m, q)

	if err := m.DeclareDurable(q); err == nil {
		t.Fatal("DeclareDurable on a queue already declared as transient succeeded, want error")
	}

	// Канал должен остаться живым: до сети мы не дошли.
	if err := m.PublishMsg([]byte(`{"n":1}`), ADD, q, "srv"); err != nil {
		t.Errorf("channel is unusable after the rejected redeclare: %v", err)
	}
}
