package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"sync"
)

var kafkaHosts = []string{"127.0.0.1:9092"}
var Producer sarama.SyncProducer
var once sync.Once

func _init() {
	once.Do(func() {
		producer, err := sarama.NewSyncProducer(kafkaHosts, nil)
		if err != nil {
			panic(err)
		}
		Producer = producer
		defer func() {
			producer.Close()
		}()
	})
}

func ProduceMessage(topic string, message []byte) error {
	producer, err := sarama.NewSyncProducer(kafkaHosts, nil)
	if err != nil {
		return err
	}
	defer func() {
		producer.Close()
	}()

	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(message)}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		Logger.Errorf("FAILED to send message: %s, topic:%s\n", err, topic)
		return errors.WithStack(err)
	}
	Logger.Info("> message sent to partition %d at offset %d, topic:%s\n", partition, offset, topic)
	return nil
}
