package kafka

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/Gabriel-Schiestl/qq-framework-basic-golang/utils"
	"github.com/Gabriel-Schiestl/qq-framework-log-golang/logger"
	kafkaGo "github.com/segmentio/kafka-go"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type KafkaMessage struct {
	Message *kafkaGo.Message
}

type KafkaConsumer struct {
	reader *kafkaGo.Reader
}

type KafkaConsumerConfig struct {
	Topic             string
	MessageDto        reflect.Type
	Handle            func(ctx context.Context, dto interface{}) error
	ConcurrentReaders int
	ServiceName       string
}

type KafkaReaderOption func(*kafkaGo.ReaderConfig)

func NewKafkaConsumer(cfg IKafkaProvider, kafkaConsumerConfig *KafkaConsumerConfig, opts ...KafkaReaderOption) error {

	ctx, cancel := context.WithCancel(context.Background())

	minBytes, err := strconv.Atoi(cfg.GetConsumerMinBytes())
	if err != nil {
		minBytes = 10e3
	}

	maxBytes, err := strconv.Atoi(cfg.GetConsumerMaxBytes())
	if err != nil {
		maxBytes = 10e6
	}

	serviceName := kafkaConsumerConfig.ServiceName
    if serviceName == "" {
        serviceName = os.Getenv("APP_NAME") + "-kafka"
    }

	kafkaReaderConfig := kafkaGo.ReaderConfig{
		Brokers:           cfg.GetBrokers(),
		GroupID:           cfg.GetGroupID(),
		Topic:             kafkaConsumerConfig.Topic,
		MinBytes:          minBytes, // 10KB
		MaxBytes:          maxBytes, // 10MB
		HeartbeatInterval: time.Duration(cfg.GetHeartbeatInterval()) * time.Second,
		SessionTimeout:    time.Duration(cfg.GetHeartbeatInterval()*cfg.GetSessionTimeoutMultiplier()) * time.Second,
		CommitInterval:    time.Duration(cfg.GetCommitInterval()) * time.Second,
		StartOffset:       kafkaGo.FirstOffset,
		ErrorLogger:       kafkaGo.LoggerFunc(logger.Get().Errorf),
	}

	for _, opt := range opts {
		opt(&kafkaReaderConfig)
	}

	var wg sync.WaitGroup

	for i := 0; i < kafkaConsumerConfig.ConcurrentReaders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			kafkaConsumer := &KafkaConsumer{
				reader: kafkaGo.NewReader(kafkaReaderConfig),
			}

			log, _ := logger.Trace(ctx)
			defer func() {
				if err := kafkaConsumer.reader.Close(); err != nil {
					log.Errorf("failed to close kafka reader: %v", err)
				}
			}()

			for {
				log, traceCtx := logger.Trace(ctx)
				message, err := kafkaConsumer.reader.ReadMessage(traceCtx)
				if err != nil {
					log.Errorf("failed to read message on topic %s, partition %d: %v", kafkaConsumerConfig.Topic, message.Partition, err)
					continue
				}

				minifiedContent, err := utils.MinifyJson(message.Value)

				if err != nil {
					log.Errorf("failed to minify json: %v", err)
					continue
				}

				dtoInstance := reflect.New(kafkaConsumerConfig.MessageDto).Interface()
				if err := json.Unmarshal(message.Value, dtoInstance); err != nil {
					log.Errorf("failed to unmarshal message: %v", err)
					continue
				}

				log.Debugf(
					"mensagem recebida: Tópico=%s, Partição=%d, Offset=%d, Timestamp=%s, Conteúdo=%s",
					kafkaConsumerConfig.Topic, message.Partition, message.Offset, message.Time.Format(time.RFC3339), minifiedContent,
				)

				processSpan, processSpanCtx := tracer.StartSpanFromContext(traceCtx, "message.process", tracer.ResourceName(kafkaConsumerConfig.Topic), tracer.ServiceName(serviceName))

				processSpan.SetTag("kafka.topic", kafkaConsumerConfig.Topic)
				processSpan.SetTag("kafka.partition", message.Partition)
				processSpan.SetTag("kafka.offset", message.Offset)

				if err := kafkaConsumerConfig.Handle(processSpanCtx, dtoInstance); err != nil {
					log.Errorf("error handling message: %v", err)
					processSpan.SetTag("error", err)
				}
				processSpan.Finish()
			}
		}()
	}
	wg.Wait()
	cancel()
	return nil
}

func WithGroupID(groupID string) KafkaReaderOption {
	return func(readerConfig *kafkaGo.ReaderConfig) {
		readerConfig.GroupID = groupID
	}
}

func WithBrokers(brokers []string) KafkaReaderOption {
	return func(readerConfig *kafkaGo.ReaderConfig) {
		readerConfig.Brokers = brokers
	}
}

func WithHeartbeatInterval(heartbeatInterval int) KafkaReaderOption {
	return func(readerConfig *kafkaGo.ReaderConfig) {
		readerConfig.HeartbeatInterval = time.Duration(heartbeatInterval) * time.Second
	}
}

func WithSessionTimeoutMultiplier(sessionTimeoutMultiplier int) KafkaReaderOption {
	return func(readerConfig *kafkaGo.ReaderConfig) {
		readerConfig.SessionTimeout = time.Duration(int(readerConfig.HeartbeatInterval.Seconds())*sessionTimeoutMultiplier) * time.Second
	}
}

func WithCommitInterval(commitInterval int) KafkaReaderOption {
	return func(readerConfig *kafkaGo.ReaderConfig) {
		readerConfig.CommitInterval = time.Duration(commitInterval) * time.Second
	}
}
