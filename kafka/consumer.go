package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"log"
	"time"
)

const KafkaTopic = "kafka_topic"
const GroupId = "kafka_group_id"

func ConsumerServer(ctx context.Context) {
	go ConsumeMessage(ctx)
	select {
	case <-ctx.Done():
	}
}

func ConsumeMessage(ctx context.Context) {
	for {
		client := &GroupConsumer{
			KafkaServer: kafkaHosts,
			ListenTopic: []string{KafkaTopic},
			GroupId:     GroupId,
			ProcessFunc: func(message *sarama.ConsumerMessage) error {
				messageMap := make(map[string]interface{})
				if err := json.Unmarshal(message.Value, &messageMap); err != nil {
					return err
				}
				Logger.Debugf("consume success \n%+v", messageMap)
				return nil
			},
		}
		err := client.Run(ctx)
		if err != nil {
			Logger.Error("consume err")
		}
		time.Sleep(5 * time.Second)
	}
}

type GroupConsumer struct {
	ProcessFunc func(*sarama.ConsumerMessage) error
	KafkaServer []string
	ListenTopic []string
	GroupId     string
}

func (GroupConsumer) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (GroupConsumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h *GroupConsumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var msg *sarama.ConsumerMessage

	for msg = range claim.Messages() {
		log.Printf("Message topic:%q partition:%d offset:%d timestamp:%v\n", msg.Topic, msg.Partition, msg.Offset, msg.Timestamp)
		err := h.ProcessFunc(msg)
		if err != nil {

			logrus.Errorf(`consumer err, %s`, err)
		}
		sess.MarkMessage(msg, "")
	}
	return nil
}

func (h *GroupConsumer) Run(ctx context.Context) error {
	if h.GroupId == "" || len(h.ListenTopic) < 1 || len(h.KafkaServer) < 1 {
		panic(fmt.Sprintf("kafka config invalid, GroupId:%s ListenTopic:%s KafkaServer:%v\n", h.GroupId, h.ListenTopic, h.KafkaServer))
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	saramaConfig.Version = sarama.V1_1_0_0

	client, err := sarama.NewClient(h.KafkaServer, saramaConfig)
	if err != nil {
		return err
	}
	defer func() {
		_ = client.Close()
	}()

	group, err := sarama.NewConsumerGroupFromClient(h.GroupId, client)
	if err != nil {
		return err
	}

	for {
		err = group.Consume(ctx, h.ListenTopic, h)
		if err != nil {
			return err
		}
	}

	return nil
}
