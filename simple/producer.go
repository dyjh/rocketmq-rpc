package simple

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"time"
)

func PushSimple(topic string, messageData string) bool {
	// 消息消费失败重试两次
	newProducer, err := rocketmq.NewProducer(producer.WithNameServer([]string{"10.0.4.9:9876"}), producer.WithRetry(2))

	if err != nil {
		fmt.Printf("生成producer失败")
		return false
	}
	if err = newProducer.Start(); err != nil {
		fmt.Printf("启动producer失败")
		return false
	}

	res, err := newProducer.SendSync(context.Background(), primitive.NewMessage("SimpleTopic", []byte("一条简单消息")))
	if err != nil {
		fmt.Printf("消息发送失败" + err.Error())
		return false
	}
	nowStr := time.Now().Format("2006-01-02 15:04:05")
	fmt.Printf("%s: 消息: %s发送成功 \n", nowStr, res.String())
	return true
}
