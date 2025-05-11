package trigger

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"timerTask/common/conf"
	"timerTask/common/model/msg"
	"timerTask/common/model/vo"
	"timerTask/common/utils"
	"timerTask/pkg/concurrency"
	"timerTask/pkg/log"
	"timerTask/pkg/msgQueue"
	"timerTask/pkg/redis"

	"github.com/hibiken/asynq"
)

type Worker struct {
	task         taskService
	confProvider confProvider
	msgServer    *msgQueue.Server
	msgClient    *msgQueue.Client
	lockService  *redis.Client
}

var defaultWorker *Worker

func NewWorker(task *TaskService, lockService *redis.Client, confProvider *conf.TriggerAppConfProvider,
	msgClient *msgQueue.Client, msgServer *msgQueue.Server) *Worker {

	defaultWorker = &Worker{
		task:         task,
		lockService:  lockService,
		confProvider: confProvider,
		msgClient:    msgClient,
		msgServer:    msgServer,
	}
	return defaultWorker
}

// 这个函数每次运行会持续 1min， 间隔1s 进行检查，看看有没有符合条件的定时任务；
// 如果存在需要执行的定时任务，发送消息到消息队列
func schedulerHandler(ctx context.Context, task *asynq.Task) error {
	var dataSt msg.SchedulerDataSt
	if err := json.Unmarshal(task.Payload(), &dataSt); err != nil {
		return err
	}

	// 进行为时一分钟的 zrange 处理
	startTime, err := getStartMinute(dataSt.TimeBucketLockKey)
	if err != nil {
		return err
	}

	conf := defaultWorker.confProvider.Get()
	// 定时1s 检查任务是否时间达标
	ticker := time.NewTicker(time.Duration(conf.ZRangeGapSeconds) * time.Second)
	defer ticker.Stop()

	endTime := startTime.Add(time.Minute)
	// channel 缓冲大小61
	notifier := concurrency.NewSafeChan(int(time.Minute/(time.Duration(conf.ZRangeGapSeconds)*time.Second)) + 1)
	defer notifier.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := defaultWorker.handleBatch(ctx, dataSt.TimeBucketLockKey, startTime, startTime.Add(time.Duration(conf.ZRangeGapSeconds)*time.Second)); err != nil {
			notifier.Put(err)
		}
	}()
	// 定时1s， 检查这一分钟内的任务那些
	for range ticker.C {
		select {
		case e := <-notifier.GetChan():
			err, _ = e.(error)
			return err
		default:
		}
		// 保证1min 后退出， 即startTime 一直增加，直到大于endTime 后退出， 表示成功消费完这个消息 （每条消息表示 这一分钟，对应的bucketID 上面的定时任务）
		if startTime = startTime.Add(time.Duration(conf.ZRangeGapSeconds) * time.Second); startTime.Equal(endTime) || startTime.After(endTime) {
			break
		}

		wg.Add(1)
		go func(startTime time.Time) {
			defer wg.Done()
			// 检查 2006-01-02 15:04_bucketID 这个key 上面的 集合是否有满足
			// startTime - startTime.Add(time.Duration(conf.ZRangeGapSeconds)*time.Second)  区间的定时任务
			if err := defaultWorker.handleBatch(ctx, dataSt.TimeBucketLockKey, startTime, startTime.Add(time.Duration(conf.ZRangeGapSeconds)*time.Second)); err != nil {
				notifier.Put(err)
			}
		}(startTime)
	}

	wg.Wait()
	select {
	case e := <-notifier.GetChan():
		err, _ = e.(error)
		return err
	default:
	}
	log.InfoContextf(ctx, "hhandler success, key: %s", dataSt.TimeBucketLockKey)
	return nil
}
func (w *Worker) Work() {
	mux := asynq.NewServeMux()
	// 注册处理函数
	mux.HandleFunc(msg.SchedulerTopic, schedulerHandler)
	// 开始处理任务
	if err := w.msgServer.Run(mux); err != nil {
		log.Errorf("msgServer run error: %v", err)
		return
	}
}

func (w *Worker) handleBatch(ctx context.Context, key string, start, end time.Time) error {
	bucket, err := getBucket(key)
	if err != nil {
		return err
	}
	// 1. 通过zrang  2006-01-02 15:04_bucketID  start end  这个范围是否有定时任务
	// 2. redis不存在，那么查询数据库 task 表
	tasks, err := w.task.GetTasksByTime(ctx, key, bucket, start, end)
	if err != nil {
		return err
	}

	timerIDs := make([]uint, 0, len(tasks))
	for _, task := range tasks {
		timerIDs = append(timerIDs, task.TimerID)
	}
	// 1. 得到满足 当前时间段内需要执行的定时任务， 那么发送消息，通知 执行器执行任务
	for _, task := range tasks {
		task := task
		dataSt := msg.TiggerDataSt{
			TimerIDUnixKey: utils.UnionTimerIDUnix(task.TimerID, task.RunTimer.UnixMilli()),
		}
		if err := w.msgClient.Send(msg.TiggerTopic, dataSt); err != nil {
			log.ErrorContextf(ctx, "trigger work failed, err: %v", err)
		}
		log.InfoContextf(ctx, "hhandler success, key: %s val:%v", msg.TiggerTopic, dataSt)
	}

	return nil
}

func getStartMinute(slice string) (time.Time, error) {
	timeBucket := strings.Split(slice, "_")
	if len(timeBucket) != 2 {
		return time.Time{}, fmt.Errorf("invalid format of msg key: %s", slice)
	}

	return utils.GetStartMinute(timeBucket[0])
}

func getBucket(slice string) (int, error) {
	timeBucket := strings.Split(slice, "_")
	if len(timeBucket) != 2 {
		return -1, fmt.Errorf("invalid format of msg key: %s", slice)
	}
	return strconv.Atoi(timeBucket[1])
}

type taskService interface {
	GetTasksByTime(ctx context.Context, key string, bucket int, start, end time.Time) ([]*vo.Task, error)
}

type confProvider interface {
	Get() *conf.TriggerAppConf
}
