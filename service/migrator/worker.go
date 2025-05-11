package service

import (
	"context"
	"time"

	mconf "timerTask/common/conf"
	"timerTask/common/consts"
	"timerTask/common/utils"
	taskdao "timerTask/dao/task"
	timerdao "timerTask/dao/timer"
	"timerTask/pkg/cron"
	"timerTask/pkg/log"
	"timerTask/pkg/redis"
)

type Worker struct {
	timerDAO          *timerdao.TimerDAO
	taskDAO           *taskdao.TaskDAO
	taskCache         *taskdao.TaskCache
	cronParser        *cron.CronParser
	lockService       *redis.Client
	appConfigProvider *mconf.MigratorAppConfProvider
}

func NewWorker(timerDAO *timerdao.TimerDAO, taskDAO *taskdao.TaskDAO, taskCache *taskdao.TaskCache, lockService *redis.Client,
	cronParser *cron.CronParser, appConfigProvider *mconf.MigratorAppConfProvider) *Worker {
	return &Worker{
		timerDAO:          timerDAO,
		taskDAO:           taskDAO,
		taskCache:         taskCache,
		lockService:       lockService,
		cronParser:        cronParser,
		appConfigProvider: appConfigProvider,
	}
}

func (w *Worker) Start(ctx context.Context) error {
	conf := w.appConfigProvider.Get()
	ticker := time.NewTicker(time.Duration(1) * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		log.InfoContext(ctx, "migrator ticking, duration is:", conf.MigrateStepMinutes)
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		locker := w.lockService.GetDistributionLock(utils.GetMigratorLockKey(utils.GetStartHour(time.Now())))
		if err := locker.Lock(ctx, int64(conf.MigrateTryLockMinutes)*int64(time.Minute/time.Second)); err != nil {
			log.ErrorContextf(ctx, "migrator get lock failed, key: %s, err: %v", utils.GetMigratorLockKey(utils.GetStartHour(time.Now())), err)
			continue
		}

		if err := w.migrate(ctx); err != nil {
			log.ErrorContextf(ctx, "migrate failed, err: %v", err)
			continue
		}

		_ = locker.ExpireLock(ctx, int64(conf.MigrateSucessExpireMinutes)*int64(time.Minute/time.Second))
	}
	return nil
}

func (w *Worker) migrate(ctx context.Context) error {
	timers, err := w.timerDAO.GetTimers(ctx, timerdao.WithStatus(int32(consts.Enabled.ToInt())))
	if err != nil {
		return err
	}

	conf := w.appConfigProvider.Get()
	now := time.Now()
	start, end := utils.GetStartHour(now.Add(time.Duration(conf.MigrateStepMinutes)*time.Minute)), utils.GetStartHour(now.Add(2*time.Duration(conf.MigrateStepMinutes)*time.Minute))
	log.InfoContextf(ctx, "******start:%v end :%v", start, end)
	// 迁移可以慢慢来，不着急
	for _, timer := range timers {
		nexts, _ := w.cronParser.NextsBetween(timer.Cron, start, end)
		if err := w.timerDAO.BatchCreateRecords(ctx, timer.BatchTasksFromTimer(nexts)); err != nil {
			log.ErrorContextf(ctx, "migrator batch create records for timer: %d failed, err: %v", timer.ID, err)
		}
		time.Sleep(5 * time.Second)
	}

	// if err := w.batchCreateBucket(ctx, start, end); err != nil {
	// 	log.ErrorContextf(ctx, "batch create bucket failed, start: %v", start)
	// 	return err
	// }

	// log.InfoContext(ctx, "migrator batch create db tasks susccess")

	return w.migrateToCache(ctx, start, end)
}

// func (w *Worker) batchCreateBucket(ctx context.Context, start, end time.Time) error {
// 	cntByMins, err := w.taskDAO.CountGroupByMinute(ctx, start.Format(consts.SecondFormat), end.Format(consts.SecondFormat))
// 	if err != nil {
// 		return err
// 	}

// 	return w.taskCache.BatchCreateBucket(ctx, cntByMins, end)
// }

func (w *Worker) migrateToCache(ctx context.Context, start, end time.Time) error {
	// 迁移完成后，将所有添加的 task 取出，添加到 redis 当中
	tasks, err := w.taskDAO.GetTasks(ctx, taskdao.WithStartTime(start), taskdao.WithEndTime(end))
	if err != nil {
		log.ErrorContextf(ctx, "migrator batch get tasks failed, err: %v", err)
		return err
	}
	// log.InfoContext(ctx, "migrator batch get tasks susccess")
	return w.taskCache.BatchCreateTasks(ctx, tasks, start, end)
}
