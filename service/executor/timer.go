package executor

import (
	"context"
	"sync"
	"time"

	"timerTask/common/conf"
	"timerTask/common/consts"
	"timerTask/common/model/po"
	"timerTask/common/model/vo"
	taskdao "timerTask/dao/task"
	timerdao "timerTask/dao/timer"
	"timerTask/pkg/log"
)

type TimerService struct {
	sync.Once
	confProvider *conf.MigratorAppConfProvider
	ctx          context.Context
	stop         func()
	timers       map[uint]*vo.Timer
	timerDAO     timerDAO
	taskDAO      *taskdao.TaskDAO
}

func NewTimerService(timerDAO *timerdao.TimerDAO, taskDAO *taskdao.TaskDAO, confProvider *conf.MigratorAppConfProvider) *TimerService {
	return &TimerService{
		confProvider: confProvider,
		timers:       make(map[uint]*vo.Timer),
		timerDAO:     timerDAO,
		taskDAO:      taskDAO,
	}
}

// 定时1min  扫描task 表，获取当前时刻到2min 后 这段时间内的任务 。然后添加到 全局对象 map[uint]*vo.Timer
func (t *TimerService) Start(ctx context.Context) {
	t.Do(func() {
		go func() {
			t.ctx, t.stop = context.WithCancel(ctx)

			stepMinutes := t.confProvider.Get().TimerDetailCacheMinutes // TimerDetailCacheMinutes 默认2min
			ticker := time.NewTicker(time.Duration(stepMinutes) * time.Minute)
			defer ticker.Stop()

			for range ticker.C {
				select {
				case <-t.ctx.Done():
					return
				default:
				}

				go func() {
					start := time.Now()
					t.timers, _ = t.getTimersByTime(ctx, start, start.Add(time.Duration(stepMinutes)*time.Minute))
				}()
			}
		}()
	})
}

func (t *TimerService) getTimersByTime(ctx context.Context, start, end time.Time) (map[uint]*vo.Timer, error) {
	tasks, err := t.taskDAO.GetTasks(ctx, taskdao.WithStartTime(start), taskdao.WithEndTime(end))
	if err != nil {
		return nil, err
	}

	timerIDs := getTimerIDs(tasks)
	if len(timerIDs) == 0 {
		return nil, nil
	}
	pTimers, err := t.timerDAO.GetTimers(ctx, timerdao.WithIDs(timerIDs), timerdao.WithStatus(int32(consts.Enabled)))
	if err != nil {
		return nil, err
	}

	return getTimersMap(pTimers)
}

func getTimerIDs(tasks []*po.Task) []uint {
	timerIDSet := make(map[uint]struct{})
	for _, task := range tasks {
		if _, ok := timerIDSet[task.TimerID]; ok {
			continue
		}
		timerIDSet[task.TimerID] = struct{}{}
	}
	timerIDs := make([]uint, 0, len(timerIDSet))
	for id := range timerIDSet {
		timerIDs = append(timerIDs, id)
	}
	return timerIDs
}

func getTimersMap(pTimers []*po.Timer) (map[uint]*vo.Timer, error) {
	vTimers, err := vo.NewTimers(pTimers)
	if err != nil {
		return nil, err
	}

	timers := make(map[uint]*vo.Timer, len(vTimers))
	for _, vTimer := range vTimers {
		timers[vTimer.ID] = vTimer
	}
	return timers, nil
}

func (t *TimerService) GetTimer(ctx context.Context, id uint) (*vo.Timer, error) {
	if vTimer, ok := t.timers[id]; ok {
		// log.InfoContextf(ctx, "get timer from local cache success, timer: %+v", vTimer)
		return vTimer, nil
	}

	log.WarnContextf(ctx, "get timer from local cache failed, timerID: %d", id)

	timer, err := t.timerDAO.GetTimer(ctx, timerdao.WithID(id))
	if err != nil {
		return nil, err
	}

	return vo.NewTimer(timer)
}

func (t *TimerService) Stop() {
	t.stop()
}

type timerDAO interface {
	GetTimer(context.Context, ...timerdao.Option) (*po.Timer, error)
	GetTimers(ctx context.Context, opts ...timerdao.Option) ([]*po.Timer, error)
}
