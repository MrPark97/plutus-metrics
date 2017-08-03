package logrus_rabbitmq

import (
	"github.com/sirupsen/logrus"
)

// RabbitMQHook to send logs via RabbitMQ
type RabbitMQHook struct {
	Writer *RabbitMQWriter
}

func NewRabbitMQHook() *RabbitMQHook {
	rabbitmqwriter := NewRabbitMQWriter()
	rabbitmqwriter.Init()

	return &RabbitMQHook{Writer: rabbitmqwriter}
}

func (hook *RabbitMQHook) Fire(entry *logrus.Entry) error {
	line := entry.Message
	byteline := []byte(line)

	switch entry.Level {
	case logrus.PanicLevel:
		hook.Writer.Level = "debug"
	case logrus.FatalLevel:
		hook.Writer.Level = "fatal"
	case logrus.ErrorLevel:
		hook.Writer.Level = "error"
	case logrus.WarnLevel:
		hook.Writer.Level = "warn"
	case logrus.InfoLevel:
		hook.Writer.Level = "info"
	case logrus.DebugLevel:
		hook.Writer.Level = "debug"
	default:
		return nil
	}

	_, err := hook.Writer.Write(byteline)

	return err
}

func (hook *RabbitMQHook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}
