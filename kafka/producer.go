package kafka

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/Gabriel-Schiestl/qq-framework-log-golang/logger"
	kafkaGo "github.com/segmentio/kafka-go"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

var _ IKafkaProducer = (*KafkaProducer)(nil)

type IKafkaProducer interface {
	SendMessage(pctx context.Context, topic string, message interface{}, key string, headers map[string]string) error
	Close() error
}

type KafkaProducer struct {
	producer *kafkaGo.Writer
	serviceName string
}

type ProducerOptions struct {
    Config      IKafkaProvider
    ServiceName string
}

func NewKafkaProducer(opts ProducerOptions) *KafkaProducer {
	serviceName := opts.ServiceName
    if serviceName == "" {
        serviceName = os.Getenv("APP_NAME") + "-kafka"
    }

	return &KafkaProducer{
		serviceName: serviceName,
		producer: &kafkaGo.Writer{
			Addr:                   kafkaGo.TCP(opts.Config.GetBrokers()...),
			Balancer:               &kafkaGo.LeastBytes{},
			BatchSize:              opts.Config.GetBatchSize(),
			BatchTimeout:           time.Duration(opts.Config.GetBatchTimeout()) * time.Millisecond,
			RequiredAcks:           kafkaGo.RequiredAcks(opts.Config.GetRequiredAcks()),
			Async:                  opts.Config.IsAsync(),
			ErrorLogger:            kafkaGo.LoggerFunc(logger.Get().Errorf),
			AllowAutoTopicCreation: true,
		},
	}
}

func (p *KafkaProducer) SendMessage(pctx context.Context, topic string, message interface{}, key string, headers map[string]string) error {
	headersConverted := convertHeaders(headers)
	messageValue, err := json.Marshal(message)
	if err != nil {
		logger.Get().Errorf("falha ao serializar mensagem: %v", err)
		return err
	}

	span, spanCtx := tracer.StartSpanFromContext(pctx, "kafka.produce", tracer.ResourceName(topic), tracer.ServiceName(os.Getenv("APP_NAME")+"-kafka"))
	defer span.Finish()
	span.SetTag("kafka.topic", topic)
	span.SetTag("kafka.key", key)

	err = p.producer.WriteMessages(
		spanCtx,
		kafkaGo.Message{
			Topic:   topic,
			Time:    time.Now(),
			Key:     []byte(key),
			Value:   messageValue,
			Headers: headersConverted,
		},
	)

	if err != nil {
		span.SetTag("error", err)
		return err
	}
	return nil
}

func convertHeaders(headers map[string]string) []kafkaGo.Header {
	var kafkaHeaders []kafkaGo.Header
	for k, v := range headers {
		kafkaHeaders = append(kafkaHeaders, kafkaGo.Header{Key: k, Value: []byte(v)})
	}
	return kafkaHeaders
}

func (p *KafkaProducer) Close() error {
	return p.producer.Close()
}
