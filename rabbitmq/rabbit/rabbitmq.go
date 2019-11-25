// @author zhangyh
// @date 2019/11/18 9:51
// @todo rabbitmq 消息队列
package rabbit

import (
    "fmt"
    "github.com/streadway/amqp"
)

// @todo rabbitmq 配置信息
// @author 张永辉 2019年11月18日12:00:20
type Params struct {
    ExchangeName string
    ExchangeType string
}

// @todo rabbitmp 信息结构体
// @author 张永辉 2019年11月18日11:59:37
type RabbitMQ struct {
    RabbitService *RabbitService
    ExchangeName  string
    ExchangeType  string
}

// @todo 创建队列实例
// @author 张永辉 2019年11月18日11:58:47
func NewRabbitMQ(params Params) *RabbitMQ {
    rabbitMQ := &RabbitMQ{}
    rabbitMQ.RabbitService = NewMQ()
    rabbitMQ.RabbitService.Channel.ExchangeDeclare(params.ExchangeName, params.ExchangeType, true, false, false, true, nil)
    rabbitMQ.ExchangeName = params.ExchangeName
    rabbitMQ.ExchangeType = params.ExchangeType
    return rabbitMQ
}

// @todo 发送rabbitmq消息
// @author 张永辉 2019年11月18日13:43:30
func (this *RabbitMQ) SendMsg(msg []byte, routeKey string) {
   this.RabbitService.Channel.Publish(this.ExchangeName, routeKey, false, false, amqp.Publishing{
        ContentType:  "application/plain",
        Body:         msg,
        DeliveryMode: 2,
    })
}

// @todo 监听处理消息队列
// @author 张永辉 2019年11月18日14:06:11
func (this *RabbitMQ) Receiver(queueName, routeKey string, callBack func([]byte) error) bool {
    fmt.Println(queueName)
    this.RabbitService.Channel.Qos(1, 0, true)
    this.RabbitService.Channel.QueueDeclare(queueName, true, false, false, true, nil)
    this.RabbitService.Channel.QueueBind(queueName, routeKey, this.ExchangeName, true, nil)
    deliveries, e := this.RabbitService.Channel.Consume(queueName, "", false, false, false, false, nil)
    if e != nil {
        fmt.Println("监听失败")
        fmt.Println(e)
        return false
    }
    for msg := range deliveries {
        go callBack(msg.Body)
        e := msg.Ack(true)
        if e != nil {
            fmt.Println("确认消息异常")
            fmt.Println(e)
            return false
        }
    }
    return true
}
