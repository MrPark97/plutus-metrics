package logrus_rabbitmq

import (
	"github.com/streadway/amqp"
	"log"
)

const (
	rabbitMQURL = "amqp://guest:guest@localhost:5672"
)

type RabbitMQWriter struct {
	*amqp.Connection
	*amqp.Channel
	Level string
}

func NewRabbitMQWriter() *RabbitMQWriter {
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatal(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	return &RabbitMQWriter{Connection: conn, Channel: ch}
}

func (r *RabbitMQWriter) Write(p []byte) (n int, err error) {
	err = r.Channel.Publish(
		"plutus-logger",    // exchange
		"plutus-metrics"+"."+r.Level, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        p,
		})
	return len(p), err
}

func (r *RabbitMQWriter) Init() {
	err := r.Channel.ExchangeDeclare(
		"plutus-logger", // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		log.Fatal(err)
	}
}
