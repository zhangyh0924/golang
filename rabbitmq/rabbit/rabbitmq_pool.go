// @author zhangyh
// @date 2019/11/21 11:17
package rabbit

import (
    "fmt"
    "github.com/streadway/amqp"
)

const (
    CONN_NUM    = 1    // 初始化连接数量
    CHANNEL_NUM = 3    // 初始化虚拟通道数量
    CONN_URL = "amqp://admin:123456@192.168.0.241:5672/"
)

// @todo mq 连接结构体
// @author 张永辉 2019年11月21日11:20:02
type MQ struct {
    Conn    *amqp.Connection
    Channel chan *amqp.Channel
}

// @todo mq 服务结构体
// @author 张永辉 2019年11月21日13:54:16
type RabbitService struct {
    Conn    *amqp.Connection
    Channel *amqp.Channel
    mq      *MQ
}

var Pools chan *MQ = make(chan *MQ, CONN_NUM)

// @todo 初始化 生成3个初始化连接，每个连接生成20个通道
// @author 张永辉 2019年11月21日14:52:26
func init() {
    for i := 0; i < CONN_NUM; i++ {
        mq := &MQ{
            Conn:    GetNewRabbitConn(),
            Channel: make(chan *amqp.Channel, CHANNEL_NUM),
        }
        CreateChannel(mq)
        Pools <- mq
    }
}

// @todo 获取rabbitmq 服务对象
// @autor zhnagyh 2019年11月21日13:45:25
func NewMQ() *RabbitService {
    rabbitService := &RabbitService{}
    mq := <-Pools
    if mq.Conn.IsClosed() {
        mq.Conn = GetNewRabbitConn()
    }
    rabbitService.Conn = mq.Conn
    rabbitService.mq = mq
    if len(mq.Channel) < 1 {
        CreateChannel(mq)
    }
    rabbitService.Channel = <-mq.Channel
    Pools <- mq
    return rabbitService
}

// @todo 关闭rabbitmq服务
// @author zhangyh 2019年11月21日13:48:47
func (rabbitService *RabbitService) Close() {
    rabbitService.mq.Channel <- rabbitService.Channel
}

// @todo 获取rabbitmq 新连接
// @author 张永辉 2019年11月21日16:37:54
func GetNewRabbitConn() *amqp.Connection {
    conn, e := amqp.Dial(CONN_URL)
    if e != nil {
        fmt.Println("连接rabbitmq失败失败…………")
        panic("连接rabbitmq失败失败")
    }
    return conn
}

// @todo 获取rabbitmq 信道
// @author 张永辉 2019年11月21日16:41:48
func CreateChannel(mq *MQ)  {
    for i := 0; i < CHANNEL_NUM; i++ {
        c, e := mq.Conn.Channel()
        if e != nil {
            fmt.Println("连接rabbitmq失败失败…………")
            panic("连接rabbitmq失败失败")
        }
        mq.Channel <- c
    }
}
