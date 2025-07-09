package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
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

// isAvroMessage verifica se a mensagem está em formato Avro
func isAvroMessage(data []byte) bool {
    if len(data) < 5 {
        return false
    }
    // Magic byte do Confluent Schema Registry é 0x00
    return data[0] == 0x00
}

// extractSchemaID extrai o schema ID do header Avro
func extractSchemaID(data []byte) (int32, error) {
    if len(data) < 5 {
        return 0, fmt.Errorf("insufficient data for schema ID")
    }
    // Schema ID está nos bytes 1-4
    schemaID := binary.BigEndian.Uint32(data[1:5])
    return int32(schemaID), nil
}

// cleanAvroPayload remove bytes não-ASCII e de controle do payload Avro
func cleanAvroPayload(payload []byte) string {
    var result []byte
    
    for _, b := range payload {
        // Pular caracteres de controle específicos que aparecem no header Avro
        if b < 32 || b == 96 { // 96 é o backtick `
            continue
        }
        
        // Manter apenas caracteres ASCII printáveis válidos
        if b >= 32 && b <= 126 {
            result = append(result, b)
        }
    }
    
    return string(result)
}

// processAvroMessage processa mensagens em formato Avro
func (kc *KafkaConsumer) processAvroMessage(data []byte, key []byte, log *logger.Event) ([]byte, error) {
    schemaID, err := extractSchemaID(data)
    if err != nil {
        return nil, fmt.Errorf("failed to extract schema ID: %v", err)
    }
    
    log.Infof("Detected Avro message with schema ID: %d", schemaID)
    
    // Pular os primeiros 5 bytes (magic + schema ID)
    payload := data[5:]
    
    // Tentar encontrar JSON válido no payload
    for i := 0; i < len(payload); i++ {
        if payload[i] == '{' {
            potentialJSON := payload[i:]
            if json.Valid(potentialJSON) {
                log.Debugf("Extracted JSON from Avro: %s", string(potentialJSON))
                return potentialJSON, nil
            }
        }
    }
    
    // Se não encontrar JSON válido, limpar o payload e reconstruir
    cleanedDescription := cleanAvroPayload(payload)
    log.Debugf("Cleaned description from Avro payload: %s", cleanedDescription)
    
    // Tentar converter SKU da key para número
    var skuValue interface{} = string(key)
    if skuNum, err := strconv.Atoi(string(key)); err == nil {
        skuValue = skuNum
    }
    
    reconstructedJSON := map[string]interface{}{
        "sku":       skuValue,
        "descricao": cleanedDescription,
    }
    
    reconstructedBytes, err := json.Marshal(reconstructedJSON)
    if err != nil {
        return nil, fmt.Errorf("failed to reconstruct JSON: %v", err)
    }
    
    log.Debugf("Reconstructed JSON from Avro: %s", string(reconstructedBytes))
    return reconstructedBytes, nil
}

func (kc *KafkaConsumer) processMessage(ctx context.Context, message kafkaGo.Message, config *KafkaConsumerConfig, serviceName string) error {
    log, traceCtx := logger.Trace(ctx)
    
    var finalJSON []byte
    var err error
    
    // Verificar se é mensagem Avro
    if isAvroMessage(message.Value) {
        finalJSON, err = kc.processAvroMessage(message.Value, message.Key, log)
        if err != nil {
            log.Errorf("failed to process Avro message: %v", err)
            return err
        }
    } else {
        // Processamento normal para mensagens JSON
        if json.Valid(message.Value) {
            finalJSON = message.Value
        } else {
            log.Errorf("message is not valid JSON and not Avro format")
            return fmt.Errorf("message is not valid JSON and not Avro format")
        }
    }
    
    // Verificar se o JSON final é válido
    if !json.Valid(finalJSON) {
        log.Errorf("final JSON is not valid: %s", string(finalJSON))
        return fmt.Errorf("final JSON is not valid")
    }
    
    messageResult := MessageResult{
        Key:   string(message.Key),
        Value: json.RawMessage(finalJSON),
    }
    
    minifiedContent, err := utils.MinifyJson(finalJSON)
    if err != nil {
        log.Errorf("failed to minify json: %v", err)
        return err
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