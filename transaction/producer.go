package main

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"time"
)

type TestListener struct{}

// ExecuteLocalTransaction 执行本地事务
// primitive.CommitMessageState : 提交
// primitive.RollbackMessageState : 回滚
// primitive.UnknowState : 触发会查函数 CheckLocalTransaction
func (t TestListener) ExecuteLocalTransaction(message *primitive.Message) primitive.LocalTransactionState {
	fmt.Println("执行本地事务")
	return primitive.UnknowState
}

// CheckLocalTransaction 回查函数
// primitive.CommitMessageState : 提交
// primitive.RollbackMessageState : 回滚
// primitive.UnknowState : 触发会查函数 CheckLocalTransaction
func (t TestListener) CheckLocalTransaction(ext *primitive.MessageExt) primitive.LocalTransactionState {
	fmt.Println("会回查函数执行")
	return primitive.CommitMessageState
}

func main() {
	newTransactionProducer, err := rocketmq.NewTransactionProducer(
		&TestListener{},
		producer.WithNameServer([]string{"10.0.4.9:9876"}),
	)
	defer func(newProducer rocketmq.TransactionProducer) {
		err := newProducer.Shutdown()
		if err != nil {
			panic("关闭producer失败")
		}
	}(newTransactionProducer)
	if err != nil {
		panic("生成producer失败")
	}
	if err = newTransactionProducer.Start(); err != nil {
		panic("启动producer失败")
	}
	res, err := newTransactionProducer.SendMessageInTransaction(context.Background(), primitive.NewMessage("TransactionTopic", []byte("这是一条事务消息")))
	if err != nil {
		panic("消息发送失败" + err.Error())
	}
	nowStr := time.Now().Format("2006-01-02 15:04:05")
	fmt.Printf("%s: 消息: %s发送成功 \n", nowStr, res.String())
	time.Sleep(time.Hour)
}
