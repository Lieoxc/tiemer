package msgQueue

import (
	"encoding/json"
	"timerTask/common/conf"

	"timerTask/pkg/log"

	"github.com/hibiken/asynq"
)

type Client struct {
	*asynq.Client
}

// GetClient 获取一个数据库客户端
func GetClient(confProvider *conf.RedisConfigProvider) (*Client, error) {
	conf := confProvider.Get()
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: conf.Address})
	return &Client{client}, nil
}

func (c *Client) Send(typeName string, data interface{}) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	task := asynq.NewTask(typeName, payload)

	// 发送任务到消息队列
	_, err = c.Enqueue(task)
	if err != nil {
		return err
	}
	return nil
}

type Server struct {
	*asynq.Server
}

// GetClient 获取一个asynq服务端
func GetServer(confProvider *conf.RedisConfigProvider) (*Server, error) {
	conf := confProvider.Get()
	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: conf.Address}, // 指定与 Redis 的连接参数
		asynq.Config{
			Concurrency: 10,
			Logger:      log.GetDefaultLogger()}, // 设定并发处理的任务数量
	)
	return &Server{srv}, nil
}
