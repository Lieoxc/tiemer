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

// 每秒触发一次，第一次触发后，对应的协程 会加一个分布式锁，锁住。 表示这个协程自己处理 这 2006-01-02 15:04_bucket 这个key 上面的定时任务
// 后面其他时刻的协程都会因为争取不到分布式锁，直接退出。 所以1min 内，其实只有bucket个协程实际处理过任务
func (w *Worker) Start(ctx context.Context) error {

	ticker := time.NewTicker(time.Duration(w.appConfProvider.Get().TryLockGapMilliSeconds) * time.Millisecond)
	defer ticker.Stop()
	log.InfoContext(ctx, "worker app is Duration: ", w.appConfProvider.Get().TryLockGapMilliSeconds)
	for range ticker.C { // 每 1000 ms 触发一次
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
	// 给这个分布式key 上锁 TryLockSeconds 默认70s
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
