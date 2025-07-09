package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
	"unicode"

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
	Handle            func(ctx context.Context, dto MessageResult) error
	ConcurrentReaders int
	ServiceName       string
}

type MessageResult struct {
	Key   string      `json:"key"`
    Value json.RawMessage `json:"value"`
}

type KafkaReaderOption func(*kafkaGo.ReaderConfig)

func NewKafkaConsumer(ctx context.Context, cfg IKafkaProvider, kafkaConsumerConfig *KafkaConsumerConfig, opts ...KafkaReaderOption) error {

	consumerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

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
    errChan := make(chan error, kafkaConsumerConfig.ConcurrentReaders)

    for i := 0; i < kafkaConsumerConfig.ConcurrentReaders; i++ {
        wg.Add(1)
        go func(readerID int) {
            defer wg.Done()

            kafkaConsumer := &KafkaConsumer{
                reader: kafkaGo.NewReader(kafkaReaderConfig),
            }

            log, _ := logger.Trace(consumerCtx)
            defer func() {
                if err := kafkaConsumer.reader.Close(); err != nil {
                    log.Errorf("failed to close kafka reader %d: %v", readerID, err)
                }
            }()

            for {
                select {
                case <-consumerCtx.Done():
                    log.Infof("Context cancelled, stopping consumer %d for topic %s", readerID, kafkaConsumerConfig.Topic)
                    return
                default:
                    readCtx, readCancel := context.WithTimeout(consumerCtx, 5*time.Second)
                    message, err := kafkaConsumer.reader.FetchMessage(readCtx)
                    readCancel()
                    
                    if err != nil {
                        if consumerCtx.Err() != nil {
                            log.Infof("Context cancelled while reading message on topic %s", kafkaConsumerConfig.Topic)
                            return
                        }
                        log.Errorf("failed to read message on topic %s: %v", kafkaConsumerConfig.Topic, err)
                        time.Sleep(1 * time.Second)
                        continue
                    }

                    if err := kafkaConsumer.processMessage(consumerCtx, message, kafkaConsumerConfig, serviceName); err != nil {
                        log.Errorf("failed to process message: %v", err)
                        continue
                    }

                    if err := kafkaConsumer.reader.CommitMessages(consumerCtx, message); err != nil {
                        log.Errorf("failed to commit message: %v", err)
                    }
                }
            }
        }(i)
    }

    done := make(chan struct{})
    go func() {
        wg.Wait()
        close(done)
    }()

    select {
    case <-consumerCtx.Done():
        logger.Get().Infof("Context cancelled, waiting for consumers to finish...")
        wg.Wait()
        return consumerCtx.Err()
    case <-done:
        return nil
    case err := <-errChan:
        cancel()
        return err
    }
}

func (kc *KafkaConsumer) processMessage(ctx context.Context, message kafkaGo.Message, config *KafkaConsumerConfig, serviceName string) error {
    log, traceCtx := logger.Trace(ctx)
    fmt.Println("Processing message:", string(message.Value))
    cleanedValue := cleanMessage(message.Value)
    
    minifiedContent, err := utils.MinifyJson(cleanedValue)
    if err != nil {
        log.Errorf("failed to minify json: %v", err)
        return err
    }

    messageResult := MessageResult{
        Key:   string(message.Key),
        Value: json.RawMessage(cleanedValue),
    }

    log.Debugf(
        "mensagem recebida: Tópico=%s, Partição=%d, Offset=%d, Timestamp=%s, Conteúdo=%s",
        config.Topic, message.Partition, message.Offset, message.Time.Format(time.RFC3339), minifiedContent,
    )

    processSpan, processSpanCtx := tracer.StartSpanFromContext(traceCtx, "message.process", 
        tracer.ResourceName(config.Topic), tracer.ServiceName(serviceName))
    defer processSpan.Finish()

    processSpan.SetTag("kafka.topic", config.Topic)
    processSpan.SetTag("kafka.partition", message.Partition)
    processSpan.SetTag("kafka.offset", message.Offset)

    if err := config.Handle(processSpanCtx, messageResult); err != nil {
        log.Errorf("error handling message: %v", err)
        processSpan.SetTag("error", err)
        return err
    }

    return nil
}

func cleanMessage(data []byte) []byte {
    // Remove null bytes e caracteres de controle
    result := make([]byte, 0, len(data))
    for _, b := range data {
        r := rune(b)
        // Manter apenas caracteres printáveis, espaços, tabs e quebras de linha
        if unicode.IsPrint(r) || r == ' ' || r == '\t' || r == '\n' || r == '\r' {
            result = append(result, b)
        }
    }
    return result
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
