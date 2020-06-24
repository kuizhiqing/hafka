package main

import (
	"flag"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	var err error
	var port string
	var group string
	var consumer string
	var producer string
	var server string
	var prefix string
	var maxbytes string
	flag.StringVar(&port, "port", os.Getenv("PORT"), "server port")
	flag.StringVar(&group, "group", os.Getenv("GROUP"), "consume group, default to api, used for all is setted")
	flag.StringVar(&consumer, "consumer", os.Getenv("CONSUMER"), "consumer config : api1:topic1,topic2;api2:topic")
	flag.StringVar(&producer, "producer", os.Getenv("PRODUCER"), "producer config : api1:topic1;api2:topic2")
	flag.StringVar(&server, "server", os.Getenv("SERVER"), "bootstrap-server")
	flag.StringVar(&prefix, "prefix", os.Getenv("PREFIX"), "prefix")
	flag.StringVar(&maxbytes, "maxbytes", os.Getenv("MAXBYTES"), "maxbytes")
	flag.Parse()
	if prefix == "" {
		prefix = "kafka"
	}

	mmax := 30000000
	if maxbytes != "" {
		if mmax, err = strconv.Atoi(maxbytes); err != nil {
			panic(err)
		}
	}
	kfkp, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": server, "message.max.bytes": mmax})
	if err != nil {
		panic(err)
	}
	go func() {
		for e := range kfkp.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					// custom failed produce handler
					log.Println("KFK produce failed", ev.TopicPartition.Error)
				}
			}
		}
	}()

	router := gin.Default()
	for _, tps := range strings.Split(consumer, ";") {
		if kv := strings.Split(tps, ":"); len(kv) == 2 {
			groupid := group
			if group == "" {
				groupid = kv[0]
			}
			kfkc, err := kafka.NewConsumer(&kafka.ConfigMap{"bootstrap.servers": server, "group.id": groupid, "auto.offset.reset": "earliest"})
			if err != nil {
				log.Println(err)
				continue
			}
			topics := strings.Split(kv[1], ",")
			kfkc.SubscribeTopics(topics, nil)
			router.GET(fmt.Sprintf("/%s/poll/%s", prefix, kv[0]), func(c *gin.Context) {
				for i := 0; i < 3; i++ {
					msg, err := kfkc.ReadMessage(2 * time.Second)
					if err == nil && msg != nil {
						c.String(http.StatusOK, string(msg.Value))
						return
					}
				}
				c.String(http.StatusNoContent, "")
			})
		}
	}
	for _, tps := range strings.Split(producer, ";") {
		if kv := strings.Split(tps, ":"); len(kv) == 2 {
			router.POST(fmt.Sprintf("/%s/feed/%s", prefix, kv[0]), func(c *gin.Context) {
				body, err := c.GetRawData()
				if err == nil && len(body) > 0 {
					kfkp.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &kv[1], Partition: kafka.PartitionAny}, Value: body}, nil)
					c.String(http.StatusOK, "")
				} else {
					c.String(http.StatusBadRequest, err.Error())
				}
			})
		}
	}
	router.NoRoute(func(c *gin.Context) {
		c.String(http.StatusNotFound, "")
	})

	err = router.Run(fmt.Sprintf(":%s", port))
	kfkp.Flush(2 * 1000)
	kfkp.Close()
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}
