package scheduler

import (
	"context"
	"time"

	"timerTask/common/conf"
	"timerTask/common/model/msg"
	"timerTask/common/utils"
	"timerTask/pkg/log"
	"timerTask/pkg/msgQueue"
	"timerTask/pkg/pool"
	"timerTask/pkg/redis"
	"timerTask/service/trigger"
)

type Worker struct {
	pool            pool.WorkerPool
	appConfProvider appConfProvider
	trigger         *trigger.Worker
	lockService     lockService
	bucketGetter    bucketGetter
	msgQueue        *msgQueue.Client
	minuteBuckets   map[string]int
}

func NewWorker(redisClient *redis.Client, appConfProvider *conf.SchedulerAppConfProvider, msgQueue *msgQueue.Client) *Worker {
	return &Worker{
		pool:            pool.NewGoWorkerPool(appConfProvider.Get().WorkersNum),
		lockService:     redisClient,
		bucketGetter:    redisClient,
		appConfProvider: appConfProvider,
		msgQueue:        msgQueue,
		minuteBuckets:   make(map[string]int),
	}
}

func (w *Worker) Start(ctx context.Context) error {

	ticker := time.NewTicker(time.Duration(w.appConfProvider.Get().TryLockGapMilliSeconds) * time.Millisecond)
	defer ticker.Stop()
	log.InfoContext(ctx, "worker app is Duration: ", w.appConfProvider.Get().TryLockGapMilliSeconds)
	for range ticker.C {
		select {
		case <-ctx.Done():
			log.WarnContext(ctx, "stopped")
			return nil
		default:
		}

		w.handleSlices(ctx)
	}
	return nil
}

func (w *Worker) handleSlices(ctx context.Context) {
	for i := 0; i < w.getValidBucket(ctx); i++ {
		w.handleSlice(ctx, i)
	}
}

// 禁用动态分桶能力
func (w *Worker) getValidBucket(ctx context.Context) int {
	return w.appConfProvider.Get().BucketsNum
}

func (w *Worker) handleSlice(ctx context.Context, bucketID int) {
	// log.InfoContextf(ctx, "scheduler_1 start: %v", time.Now())
	now := time.Now()

	//处理1min之前
	if err := w.pool.Submit(func() {
		w.asyncHandleSlice(ctx, now.Add(-time.Minute), bucketID)
	}); err != nil {
		log.ErrorContextf(ctx, "[handle slice] submit task failed, err: %v", err)
	}

	//处理当前时刻
	if err := w.pool.Submit(func() {
		w.asyncHandleSlice(ctx, now, bucketID)
	}); err != nil {
		log.ErrorContextf(ctx, "[handle slice] submit task failed, err: %v", err)
	}
	// log.InfoContextf(ctx, "scheduler_1 end: %v", time.Now())
}

func (w *Worker) asyncHandleSlice(ctx context.Context, t time.Time, bucketID int) {
	locker := w.lockService.GetDistributionLock(utils.GetTimeBucketLockKey(t, bucketID))
	if err := locker.Lock(ctx, int64(w.appConfProvider.Get().TryLockSeconds)); err != nil {
		// log.WarnContextf(ctx, "get lock failed, err: %v, key: %s", err, utils.GetTimeBucketLockKey(t, bucketID))
		return
	}

	log.InfoContextf(ctx, "get scheduler lock success, key: %s", utils.GetTimeBucketLockKey(t, bucketID))

	// 创建一个任务队列，等待被消费
	dataSt := msg.SchedulerDataSt{
		TimeBucketLockKey: utils.GetSliceMsgKey(t, bucketID),
	}
	if err := w.msgQueue.Send(msg.SchedulerTopic, dataSt); err != nil {
		log.ErrorContextf(ctx, "trigger work failed, err: %v", err)
	}
	log.InfoContextf(ctx, "send msg to:%s", msg.SchedulerTopic)
}

type appConfProvider interface {
	Get() *conf.SchedulerAppConf
}

type lockService interface {
	GetDistributionLock(key string) redis.DistributeLocker
}

type bucketGetter interface {
	Get(ctx context.Context, key string) (string, error)
}
