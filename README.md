# broker

Тонкая обёртка над RabbitMQ (AMQP 0-9-1) для TunnelBroker: `web` публикует задания,
`tunnelbroker-agent` их потребляет. Exchange по умолчанию, routing key = имя очереди.

## API

| Функция | Назначение |
|---|---|
| `MsgBrokerInitDurable(dsn, queues)` | подключиться и объявить **устойчивые** очереди |
| `DeclareDurable(name)` | объявить ещё одну устойчивую очередь; повторный вызов — no-op |
| `DeclareTransient(name)` | объявить неустойчивую очередь — для legacy-очередей `6in4_<ip>` на время перехода |
| `PublishPersistent(data, msgType, queue, dstSrv)` | публикация с `DeliveryMode=Persistent`; неизвестная очередь объявляется автоматически |
| `RegisterConsumers()` | подписка на **все** объявленные очереди, сведённые в один канал, **без auto-ack** |
| `Queues()` | имена объявленных очередей |

Тип операции едет в свойстве AMQP `Type` (`add`/`delete`/`update`/`pause`/`resume`),
адресат — в `AppId`.

### Обязанность потребителя

`RegisterConsumers` выключает auto-ack. Для каждого `amqp.Delivery` нужно вызвать
`d.Ack(false)` после успешной обработки либо `d.Nack(false, requeue)` при ошибке.
Без этого сообщение вернётся в очередь при обрыве соединения — что и требуется,
но «забытые» сообщения будут копиться как unacked.

### Устаревшее

`MsgBrokerInit`, `PublishMsg`, `RegisterConsumer` оставлены ради совместимости с уже
развёрнутым парком. Их проблемы:

* очереди объявляются `durable=false` — задания теряются при перезапуске брокера;
* `RegisterConsumer` слушает **только последнюю** объявленную очередь;
* auto-ack подтверждает сообщение до обработки — падение обработчика теряет задание.

Переобъявить существующую очередь с другим флагом `durable` нельзя: RabbitMQ отвечает
`PRECONDITION_FAILED` и закрывает канал. Поэтому переход на устойчивую доставку сделан
через **новые имена очередей** (`agent.v2.<server>`), а не переключением старых.
`queueDeclare` ловит такую попытку локально, не доводя до обрыва канала.

## Тесты

Интеграционные, требуют живого RabbitMQ:

```sh
docker run -d --rm --name broker-test -p 5673:5672 rabbitmq:3.10.7-management
BROKER_TEST_DSN="amqp://guest:guest@127.0.0.1:5673/" go test ./... -v
docker stop broker-test
```

Без `BROKER_TEST_DSN` тесты пропускаются.

## Релиз

`web` и `agent` подключают этот пакет как модуль `github.com/alexvancasper/broker`.
Внутри workspace (`TunnelBroker/go.work`) он резолвится локально, но сборка в CI идёт
по версии из `go.mod`. После изменения API:

```sh
git tag v1.1.0 && git push origin main --tags
# затем в web/ и agent/:
go get github.com/alexvancasper/broker@v1.1.0
```
