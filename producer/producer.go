package main

import (
	"fmt"
	"github.com/chris-han-nih/avro-kafka-golang/producer/config"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"math/rand"
	"os"
)

func main() {
	configFile := os.Getenv("CONFIG-FILE")
	conf := config.ReadConfig(configFile)

	p, err := kafka.NewProducer(&conf)

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	delivery_chan := make(chan kafka.Event, 10000)
	topic := "test"
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},

		Value: []byte("Test Message")},
		delivery_chan,
	)

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Successfully produced record to topic %s partition [%d] @ offset %v\n", *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
				}
			}
		}
	}()

	users := [...]string{"eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"}
	items := [...]string{"book", "alarm clock", "t-shirts", "gift card", "batteries"}

	for n := 0; n < 100000; n++ {
		key := users[rand.Intn(len(users))]
		data := items[rand.Intn(len(items))]
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(key),
			Value: []byte(data),
		}, nil)
		if err != nil {
			fmt.Printf("Produce exception: %s\n", err.Error())
		}
	}

	p.Flush(1000)
	p.Close()
}
