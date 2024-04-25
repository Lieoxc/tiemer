package executor

import (
	"context"
	"encoding/json"
	"fmt"
	nethttp "net/http"
	"strings"
	"time"

	"timerTask/common/consts"
	"timerTask/common/model/msg"
	"timerTask/common/model/vo"
	"timerTask/common/utils"
	taskdao "timerTask/dao/task"
	"timerTask/pkg/bloom"
	"timerTask/pkg/log"
	"timerTask/pkg/msgQueue"
	"timerTask/pkg/promethus"
	"timerTask/pkg/xhttp"

	"github.com/hibiken/asynq"
)

type Worker struct {
	timerService *TimerService
	taskDAO      *taskdao.TaskDAO
	httpClient   *xhttp.JSONClient
	bloomFilter  *bloom.Filter
	reporter     *promethus.Reporter
	msgServer    *msgQueue.Server
}

var defaultWorker *Worker

func NewWorker(timerService *TimerService, taskDAO *taskdao.TaskDAO, httpClient *xhttp.JSONClient,
	bloomFilter *bloom.Filter, reporter *promethus.Reporter, msgServer *msgQueue.Server) *Worker {
	defaultWorker = &Worker{
		timerService: timerService,
		taskDAO:      taskDAO,
		httpClient:   httpClient,
		bloomFilter:  bloomFilter,
		reporter:     reporter,
		msgServer:    msgServer,
	}
	return defaultWorker
}

func (w *Worker) Start(ctx context.Context) {
	log.InfoContextf(ctx, "executor start: %v", time.Now())
	w.timerService.Start(ctx)
}

func executorHandler(ctx context.Context, task *asynq.Task) error {
	var dataSt msg.TiggerDataSt
	if err := json.Unmarshal(task.Payload(), &dataSt); err != nil {
		return err
	}
	log.InfoContext(ctx, "executorHandler GET msg:%s", dataSt.TimerIDUnixKey)
	// 拿到消息，查询一次完整的 timer 定义 
	timerID, unix, err := utils.SplitTimerIDUnix(dataSt.TimerIDUnixKey)
	if err != nil {
		return err
	}
	if exist, err := defaultWorker.bloomFilter.Exist(ctx, utils.GetTaskBloomFilterKey(utils.GetDayStr(time.UnixMilli(unix))), dataSt.TimerIDUnixKey); err != nil || exist {
		log.WarnContextf(ctx, "bloom filter check failed, start to check db, bloom key: %s, timerIDUnixKey: %s, err: %v, exist: %t",
			utils.GetTaskBloomFilterKey(utils.GetDayStr(time.UnixMilli(unix))), dataSt.TimerIDUnixKey, err, exist)
		// 查库判断定时器状态
		task, err := defaultWorker.taskDAO.GetTask(ctx, taskdao.WithTimerID(timerID), taskdao.WithRunTimer(time.UnixMilli(unix)))
		if err == nil && task.Status != consts.NotRunned.ToInt() {
			// 重复执行的任务
			log.WarnContextf(ctx, "task is already executed, timerID: %d, exec_time: %v", timerID, task.RunTimer)
			return nil
		}
	}

	return defaultWorker.executeAndPostProcess(ctx, timerID, unix)
}
func (w *Worker) Work() {
	mux := asynq.NewServeMux()
	// 注册处理函数
	mux.HandleFunc(msg.TiggerTopic, executorHandler)
	// 开始处理任务
	if err := w.msgServer.Run(mux); err != nil {
		log.Errorf("msgServer run error: %v", err)
		return
	}
}

func (w *Worker) executeAndPostProcess(ctx context.Context, timerID uint, unix int64) error {
	// 未执行，则查询 timer 完整的定义，执行回调
	timer, err := w.timerService.GetTimer(ctx, timerID)
	if err != nil {
		return fmt.Errorf("get timer failed, id: %d, err: %w", timerID, err)
	}

	// 定时器已经处于去激活态，则无需处理任务
	if timer.Status != consts.Enabled {
		log.WarnContextf(ctx, "timer has alread been unabled, timerID: %d", timerID)
		return nil
	}

	execTime := time.Now()
	resp, err := w.execute(ctx, timer)
	log.InfoContextf(ctx, "execute timer: %d, resp: %v, err: %v", timerID, resp, err)
	return w.postProcess(ctx, resp, err, timer.App, timerID, unix, execTime)
}

func (w *Worker) execute(ctx context.Context, timer *vo.Timer) (map[string]interface{}, error) {
	var (
		resp map[string]interface{}
		err  error
	)
	log.InfoContextf(ctx, "data:%v:%v:%v", timer.NotifyHTTPParam.URL, timer.NotifyHTTPParam.Method, timer.NotifyHTTPParam.Header)
	switch strings.ToUpper(timer.NotifyHTTPParam.Method) {
	case nethttp.MethodGet:
		err = w.httpClient.Get(ctx, timer.NotifyHTTPParam.URL, timer.NotifyHTTPParam.Header, nil, &resp)
	case nethttp.MethodPatch:
		err = w.httpClient.Patch(ctx, timer.NotifyHTTPParam.URL, timer.NotifyHTTPParam.Header, timer.NotifyHTTPParam.Body, &resp)
	case nethttp.MethodDelete:
		err = w.httpClient.Delete(ctx, timer.NotifyHTTPParam.URL, timer.NotifyHTTPParam.Header, timer.NotifyHTTPParam.Body, &resp)
	case nethttp.MethodPost:
		err = w.httpClient.Post(ctx, timer.NotifyHTTPParam.URL, timer.NotifyHTTPParam.Header, timer.NotifyHTTPParam.Body, &resp)
	default:
		err = fmt.Errorf("invalid http method: %s, timer: %s", timer.NotifyHTTPParam.Method, timer.Name)
	}

	return resp, err
}

func (w *Worker) postProcess(ctx context.Context, resp map[string]interface{}, execErr error, app string, timerID uint, unix int64, execTime time.Time) error {
	go w.reportMonitorData(app, unix, execTime)
	if err := w.bloomFilter.Set(ctx, utils.GetTaskBloomFilterKey(utils.GetDayStr(time.UnixMilli(unix))), utils.UnionTimerIDUnix(timerID, unix), consts.BloomFilterKeyExpireSeconds); err != nil {
		log.ErrorContextf(ctx, "set bloom filter failed, key: %s, err: %v", utils.GetTaskBloomFilterKey(utils.GetDayStr(time.UnixMilli(unix))), err)
	}

	task, err := w.taskDAO.GetTask(ctx, taskdao.WithTimerID(timerID), taskdao.WithRunTimer(time.UnixMilli(unix)))
	if err != nil {
		return fmt.Errorf("get task failed, timerID: %d, runTimer: %d, err: %w", timerID, time.UnixMilli(unix), err)
	}

	respBody, _ := json.Marshal(resp)
	task.Output = string(respBody)

	if execErr != nil {
		task.Status = consts.Failed.ToInt()
	} else {
		task.Status = consts.Successed.ToInt()
	}

	return w.taskDAO.UpdateTask(ctx, task)
}

func (w *Worker) reportMonitorData(app string, expectExecTimeUnix int64, acutalExecTime time.Time) {
	w.reporter.ReportExecRecord(app)
	// 上报毫秒
	w.reporter.ReportTimerDelayRecord(app, float64(acutalExecTime.UnixMilli()-expectExecTimeUnix))
}
