package main

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"time"
)

func main() {
	newPushConsumer, err := rocketmq.NewPushConsumer(consumer.WithNameServer([]string{"10.0.4.9:9876"}), consumer.WithGroupName("test"))
	defer func(newPushConsumer rocketmq.PushConsumer) {
		err := newPushConsumer.Shutdown()
		if err != nil {
			panic("关闭consumer失败")
		}
	}(newPushConsumer)

	err = newPushConsumer.Subscribe("DelayTopic", consumer.MessageSelector{}, func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			nowStr := time.Now().Format("2006-01-02 15:04:05")
			fmt.Printf("%s 读取到一条消息,消息内容: %s \n", nowStr, string(msg.Body))
		}
		return consumer.ConsumeSuccess, nil
	})

	if err != nil {
		fmt.Println("读取消息失败")
	}
	if err = newPushConsumer.Start(); err != nil {
		panic("启动consumer失败")
	}
	// 不能让主goroutine退出
	time.Sleep(time.Hour)
}
