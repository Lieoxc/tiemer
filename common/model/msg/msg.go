package msg

const SchedulerTopic = "schedulerTopic:start"

type SchedulerDataSt struct {
	TimeBucketLockKey string `json:"key"`
}

const TiggerTopic = "tiggerTopic:start"

type TiggerDataSt struct {
	TimerIDUnixKey string `json:"key"`
}
