package kafka

type KafkaTopics map[string]string

type IKafkaProvider interface {
	GetBrokers() []string
	GetConsumerMinBytes() string
	GetConsumerMaxBytes() string
	GetHeartbeatInterval() int
	GetConcurrentReaders() int
	GetGroupID() string
	GetSessionTimeoutMultiplier() int
	GetCommitInterval() int
	GetBatchSize() int
	GetBatchTimeout() int
	GetRequiredAcks() int
	IsAsync() bool
	GetTopics() KafkaTopics
	GetTopicName(topic string) string
}