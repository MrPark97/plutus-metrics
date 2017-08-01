package main

import (
	"github.com/streadway/amqp"
)

type RabbitMQWriter struct {
	*amqp.Connection
	*amqp.Channel
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

	return &RabbitMQWriter{conn, ch}
}

func (r *RabbitMQWriter) Write(p []byte) (n int, err error) {
	err = r.Channel.Publish(
		"fm-metrics",                // exchange
		"fm-metrics-service.system", // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        p,
		})
	return len(p), err
}

func (r *RabbitMQWriter) Init() {
	err := r.Channel.ExchangeDeclare(
		"logs_topic", // name
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
