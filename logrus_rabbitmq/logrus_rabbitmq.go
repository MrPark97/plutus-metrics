package logrus_rabbitmq

import (
	"github.com/sirupsen/logrus"
	"log"
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
	line, err := entry.String()
	byteline := []byte(line)
	if err != nil {
		log.Println("Unable to read entry, " + err.Error())
	}

	switch entry.Level {
	case logrus.PanicLevel:
		_, err = hook.Writer.Write(byteline)
		return err
	case logrus.FatalLevel:
		_, err = hook.Writer.Write(byteline)
		return err
	case logrus.ErrorLevel:
		_, err = hook.Writer.Write(byteline)
		return err
	case logrus.WarnLevel:
		_, err = hook.Writer.Write(byteline)
		return err
	case logrus.InfoLevel:
		_, err = hook.Writer.Write(byteline)
		return err
	case logrus.DebugLevel:
		_, err = hook.Writer.Write(byteline)
		return err
	default:
		return nil
	}
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
