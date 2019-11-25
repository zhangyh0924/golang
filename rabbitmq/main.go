package main

import (
    "fmt"
    "strconv"
    "time"
    "rabbitmq/rabbit"
)

func main() {
    // 创建mq 处理对象
    mq := rabbit.NewRabbitMQ(rabbit.Params{
        ExchangeName: "test_exchange",
        ExchangeType: "topic",
    })

    // 测试发送消息
    go func() {
     test := "hello word->"
     for i := 0; i < 1000; i++ {
         mq.SendMsg([]byte(test+strconv.Itoa(i)), "test.route")
         time.Sleep(time.Millisecond *500)
     }
    }()
    //测试接收消息
   go mq.Receiver("test_queue", "test.route", func(bytes []byte) error {
     fmt.Printf("msg->%s\n",string(bytes))
     return nil
   })

    go mq.Receiver("test_queue1", "test.#", func(bytes []byte) error {
        fmt.Printf("msg-1->%s\n",string(bytes))
        return nil
    })

    for {
        time.Sleep(time.Second *100)
    }

}
