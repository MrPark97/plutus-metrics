package main

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/rcrowley/go-metrics"
	"github.com/rubyist/circuitbreaker"
	"github.com/streadway/amqp"
)

// Init influx and rabbit URLs for connection
var (
	rabbitMQURL = "amqp://guest:guest@localhost:5672"
	influxURL   = "http://localhost:8086"
	log         = logrus.New()
)

// Define rabbitMQ struct for receiving messages with consecutive circuit breaker
type RabbitMQ struct {
	rmq       *amqp.Connection // rabbitMQ connection
	host      string           // url for rabbitMQ connection
	timeout   time.Duration    // func execution time out for circuit breaker
	ErrChan   chan error       // errors consumer channel
	rmqc      *amqp.Channel    // rabbitMQ channel
	circBreak *circuit.Breaker // circuit breaker
}

// Create new rabbit struct init Exchange Queue and Consume messages
func NewRabbitMQ(host string, timeout time.Duration, cb *circuit.Breaker) (*RabbitMQ, error) {
	// init rabbitMQ struct
	rm := &RabbitMQ{
		host:      host,
		timeout:   timeout,
		circBreak: cb,
		ErrChan:   make(chan error),
	}
	// init errors consumer
	failOnError(rm.ErrChan)

	// try to connect and init RabbitMQ if circuit breaker is not tripped
	var err error
	if !rm.circBreak.Tripped() {
		err = rm.Connect()
		if err != nil {
			return rm, err
		}
		err = rm.Listen()
	}
	return rm, err
}

// connect to rabbitMQ
func (r *RabbitMQ) Connect() error {

	var (
		err  error
		conn *amqp.Connection
	)

	// Creates a connection to RabbitMQ
	r.circBreak.Call(func() error {
		conn, err = amqp.Dial(rabbitMQURL)
		r.rmq = conn
		if err != nil {
			return ErrConnect(err.Error())
		}
		return nil
	}, r.timeout)
	if err != nil {
		return ErrConnect(err.Error())
	}
	return err
}

// Declare exchange queue bind queue with exchange and consume messages from RabbitMQ
func (r *RabbitMQ) Listen() error {
	ch, err := r.rmq.Channel()
	r.rmqc = ch

	err = ch.ExchangeDeclare(
		"fm-metrics", // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unsused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)

	err = ch.QueueBind(
		q.Name,       // queue name
		"#",          // routing key
		"fm-metrics", // exchange
		false,
		nil)

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)

	// Listen in new goroutine
	go func() {
		// create new consecutive breaker for InfluxDB
		cb := circuit.NewConsecutiveBreaker(10)

		// init circuit breaker listener
		// Subscribe to the circuit breaker events
		events := cb.Subscribe()

		var CurrentMessage amqp.Delivery
		go func() {
			for {
				e := <-events
				switch e {
				case circuit.BreakerTripped:
					cb.Reset()
					log.Fatalln("[x] service stopped")
				case circuit.BreakerFail:
					err := writeMetricsToInfluxDB(CurrentMessage.Body, CurrentMessage.RoutingKey, r.ErrChan, cb)
					if err != nil {
						r.ErrChan <- err
					}
					continue
				}
			}
		}()

		for d := range msgs {
			CurrentMessage = d
			log.Infof(" [x] %s\n %s\n\n\n", d.RoutingKey, d.Body)

			// write metrics to influxdb send error or acknnowledge
			err := writeMetricsToInfluxDB(d.Body, d.RoutingKey, r.ErrChan, cb)
			if err != nil {
				r.ErrChan <- err
			} else {
				d.Ack(false)
			}
		}
	}()
	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	return err
}

// Describes new error type ErrWrongKey which used if routing key in metrics is wrong
type ErrWrongKey string

func (e ErrWrongKey) Error() string {
	return "[x] wrong routing key [" + string(e) + "]"
}

// Describes new error type ErrConnect which used if failed to connect remote services
type ErrConnect string

func (e ErrConnect) Error() string {
	return string(e)
}

func main() {
	// init rabbitMQ circuit breaker
	// init event listener
	// Subscribe to the circuit breaker events
	rcb := circuit.NewConsecutiveBreaker(10)
	events := rcb.Subscribe()

	// The circuit breaker events handling
	go func() {
		for {
			e := <-events
			switch e {
			case circuit.BreakerTripped:
				rcb.Reset()
				log.Fatal("[x] service stopped")
			case circuit.BreakerFail:
				rmq, err := NewRabbitMQ(rabbitMQURL, time.Second*10, rcb)
				if err != nil && rmq.ErrChan != nil {
					rmq.ErrChan <- err
				}
				continue
			}
		}
	}()

	// try to create and init RabbitMQ with circuit breaker timeout 10 and threashold 10
	rmq, err := NewRabbitMQ(rabbitMQURL, time.Second*10, rcb)
	if err != nil {
		if rmq.ErrChan != nil {
			rmq.ErrChan <- err
		} else {
			log.Println(err.Error())
		}
	}
	// try to defer close rabbitMQ channel and connection if exist
	if rmq.rmq != nil {
		defer rmq.rmq.Close()
	}
	if rmq.rmqc != nil {
		defer rmq.rmqc.Close()
	}

	//init self system metrics
	sysMetrics := metrics.NewRegistry()
	metrics.RegisterDebugGCStats(sysMetrics)
	metrics.RegisterRuntimeMemStats(sysMetrics)

	// init rabbitmqwriter which will write this service metrics
	rabbitmqwriter := NewRabbitMQWriter()
	rabbitmqwriter.Init()

	// periodically capture metrics values and write to rabbitMQ
	metricsDuration := time.Second * 10
	go metrics.CaptureDebugGCStats(sysMetrics, metricsDuration)
	go metrics.CaptureRuntimeMemStats(sysMetrics, metricsDuration)
	go metrics.WriteJSON(sysMetrics, metricsDuration, rabbitmqwriter)

	// wait messages from rabbitMQ
	forever := make(chan bool)
	<-forever

}

// Is used for error handling
func failOnError(errChan <-chan error) {
	go func() {
		for {
			err := <-errChan
			if err != nil {
				switch err.(type) {
				case ErrWrongKey:
					log.Printf(err.Error())
				case ErrConnect:
					log.Printf(err.Error())
				default:
					log.Fatalf(err.Error())
				}
			}
		}
	}()
}

// Unmarshall metrics from JSON and write metrics to InfluxDB
func writeMetricsToInfluxDB(jsonData []byte, routingKey string, errChan chan<- error, cb *circuit.Breaker) error {
	// unmarshall []byte to new map
	metricsData := make(map[string]map[string]interface{})
	json.Unmarshal(jsonData, &metricsData)

	// check routingKey format
	sepCounter := strings.Count(routingKey, `.`)
	routingSubKeys := make([]string, 2)
	if sepCounter == 1 {
		routingSubKeys = strings.Split(routingKey, `.`)
	} else {
		return ErrWrongKey(routingKey)
	}

	// Create a new point batch
	batchPoints, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  "mydb",
		Precision: "s",
	})

	// prepare metrics for writing to InfluxDB
	for k, v := range metricsData {
		tags := map[string]string{
			"service": routingSubKeys[0],
			"status":  routingSubKeys[1],
		}
		fields := v

		pt, _ := client.NewPoint(k, tags, fields, time.Now())

		batchPoints.AddPoint(pt)
	}

	var err error
	// try to connect to InfluxDB and send metrics
	cb.Call(func() error {
		influxDB, err := client.NewHTTPClient(client.HTTPConfig{
			Addr:     influxURL,
			Username: "root",
			Password: "root",
		})
		if err != nil {
			return ErrConnect(err.Error())
		}

		err = influxDB.Write(batchPoints)

		if err != nil {
			return ErrConnect(err.Error())
		}

		return nil
	}, time.Second*10)
	if err != nil {
		return ErrConnect(err.Error())
	}

	return err
}
