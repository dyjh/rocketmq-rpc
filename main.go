package main

import (
	"go-rocketmq/delay"
	"go-rocketmq/simple"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
)

// Golang RPC 的实现需要 5 个步骤
// 1. 定义一个服务结构
// 2. 为这个服务结构定义几个服务方法，每个方法接受两个参数和返回 error 类型
// 3. 使用 rpc.RegisterName() 方法注册 「服务结构」 的实例
// 4. 监听套接字
// 5. 为每一个套接字调用 jsonrpc.ServerConn(conn) 方法

type RocketService struct {
}

// 定义 RocketService 所需要的参数，一般是两个，string 类型
type Args struct {
	Message string
}

// 2.
// 实现延时消息，需要两个参数
// 所有的 jsonrpc 方法只有两个参数，第一个参数用于接收所有参数，
// 第二个参数用于处理返回结果，是一个指针
// 所有的 jsonrpc 都只有一个返回值，error,用于指示是否发生错误
func (that *RocketService) Delay(args Args, reply *bool) error {
	*reply = delay.PushDelay(args.Message)
	return nil
}

// 实现即时推送服务
func (that *RocketService) Simple(args Args, reply *bool) error {
	*reply = simple.PushSimple(args.Message)
	return nil
}

// Hello
func (that *RocketService) Hello(request string, reply *string) error {
	*reply = "hello " + request
	return nil
}

func main() {
	// 3.
	rpc.RegisterName("Rocketmq", new(RocketService))
	// 4.
	sock, err := net.Listen("tcp", ":8082")
	log.Println("listen at :8082")
	if err != nil {
		log.Fatal("listen error:", err)
	}

	for {
		conn, err := sock.Accept()
		if err != nil {
			continue
		}
		// 5.
		go jsonrpc.ServeConn(conn)
	}

}