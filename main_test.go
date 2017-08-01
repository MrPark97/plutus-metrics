package main

import (
	"encoding/json"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/rcrowley/go-metrics"
	"github.com/rubyist/circuitbreaker"
	"reflect"
	"testing"
	"time"
)

func TestWriteMetricsToInfluxDB(t *testing.T) {
	var stupidCounter = metrics.NewCounter()
	var cpuUsagePercents = metrics.NewGauge()
	var executionTime = metrics.NewGauge()
	var routinesNumber = metrics.NewGauge()
	var counterPerSecond = metrics.NewGauge()

	metrics.Register("stupid_counter", stupidCounter)
	metrics.Register("CPU_usage", cpuUsagePercents)
	metrics.Register("execution_time", executionTime)
	metrics.Register("routines_number", routinesNumber)
	metrics.Register("counter_per_second", counterPerSecond)

	stupidCounter.Inc(int64(1))
	cpuUsagePercents.Update(int64(10))
	executionTime.Update(int64(100))
	routinesNumber.Update(int64(10))
	counterPerSecond.Update(int64(1000))

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
			log.Infoln(err.Error())
		}
	}
	// try to defer close rabbitMQ channel and connection if exist
	if rmq.rmq != nil {
		defer rmq.rmq.Close()
	}
	if rmq.rmqc != nil {
		defer rmq.rmqc.Close()
	}

	rabbitmqwriter := NewRabbitMQWriter()
	rabbitmqwriter.Init()
	metrics.WriteJSONOnce(metrics.DefaultRegistry, rabbitmqwriter)
	influxURL   := "http://localhost:8086"
	influxDB, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     influxURL,
		Username: "root",
		Password: "root",
	})
	if err != nil {
		log.Fatal(err)
	}
	tables := []string{"CPU_usage", "execution_time", "routines_number", "counter_per_second", "stupid_counter"}
	testTable := map[string]int64{
		"CPU_usage":          10,
		"execution_time":     100,
		"routines_number":    10,
		"counter_per_second": 1000,
		"stupid_counter":     1,
	}
	answers := make(map[string]int64)
	for i := range tables {
		var value string
		if tables[i] != "stupid_counter" {
			value = "value"
		} else {
			value = "count"
		}
		query := client.Query{
			Command:  "SELECT last(" + value + ") FROM " + tables[i],
			Database: "mydb",
		}
		response, err := influxDB.Query(query)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("%+v\n", response.Results[0].Series[0].Values[0][1])
		jsonNumberValue := response.Results[0].Series[0].Values[0][1].(json.Number)
		intValue, err := jsonNumberValue.Int64()
		if err != nil {
			log.Fatal(err)
		}
		answers[tables[i]] = intValue
	}
	equal := reflect.DeepEqual(testTable, answers)
	if !equal {
		log.Fatal("Wrong answer")
	}
}
