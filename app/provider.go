package app

import (
	"go.uber.org/dig"

	"timerTask/app/migrator"
	"timerTask/app/scheduler"
	"timerTask/app/webserver"
	"timerTask/common/conf"
	taskdao "timerTask/dao/task"
	timerdao "timerTask/dao/timer"
	"timerTask/pkg/bloom"
	"timerTask/pkg/cron"
	"timerTask/pkg/hash"
	"timerTask/pkg/msgQueue"
	"timerTask/pkg/mysql"
	"timerTask/pkg/promethus"
	"timerTask/pkg/redis"
	"timerTask/pkg/xhttp"
	executorservice "timerTask/service/executor"
	migratorservice "timerTask/service/migrator"
	schedulerservice "timerTask/service/scheduler"
	triggerservice "timerTask/service/trigger"
	webservice "timerTask/service/webserver"
)

var (
	container *dig.Container
)

func init() {
	container = dig.New()

	provideConfig(container)
	providePKG(container)
	provideDAO(container)
	provideService(container)
	provideApp(container)
}

func provideConfig(c *dig.Container) {
	c.Provide(conf.DefaultMysqlConfProvider)
	c.Provide(conf.DefaultSchedulerAppConfProvider)
	c.Provide(conf.DefaultTriggerAppConfProvider)
	c.Provide(conf.DefaultWebServerAppConfProvider)
	c.Provide(conf.DefaultRedisConfigProvider)
	c.Provide(conf.DefaultMigratorAppConfProvider)
}

func providePKG(c *dig.Container) {
	c.Provide(bloom.NewFilter)
	c.Provide(hash.NewMurmur3Encryptor)
	c.Provide(hash.NewSHA1Encryptor)
	c.Provide(redis.GetClient)
	c.Provide(mysql.GetClient)
	c.Provide(cron.NewCronParser)
	c.Provide(xhttp.NewJSONClient)
	c.Provide(promethus.GetReporter)
	c.Provide(msgQueue.GetClient)
	c.Provide(msgQueue.GetServer)
}

func provideDAO(c *dig.Container) {
	c.Provide(timerdao.NewTimerDAO)
	c.Provide(taskdao.NewTaskDAO)
	c.Provide(taskdao.NewTaskCache)
}

func provideService(c *dig.Container) {
	c.Provide(migratorservice.NewWorker)
	c.Provide(migratorservice.NewWorker)
	c.Provide(webservice.NewTaskService)
	c.Provide(webservice.NewTimerService)
	c.Provide(executorservice.NewTimerService)
	c.Provide(executorservice.NewWorker)
	c.Provide(triggerservice.NewWorker)
	c.Provide(triggerservice.NewTaskService)
	c.Provide(schedulerservice.NewWorker)
}

func provideApp(c *dig.Container) {
	c.Provide(migrator.NewMigratorApp)
	c.Provide(webserver.NewTaskApp)
	c.Provide(webserver.NewTimerApp)
	c.Provide(webserver.NewServer)
	c.Provide(scheduler.NewWorkerApp)

}

func GetSchedulerApp() *scheduler.WorkerApp {
	var schedulerApp *scheduler.WorkerApp
	if err := container.Invoke(func(_s *scheduler.WorkerApp) {
		schedulerApp = _s
	}); err != nil {
		panic(err)
	}
	return schedulerApp
}

func GetWebServer() *webserver.Server {
	var server *webserver.Server
	if err := container.Invoke(func(_s *webserver.Server) {
		server = _s
	}); err != nil {
		panic(err)
	}
	return server
}

func GetMigratorApp() *migrator.MigratorApp {
	var migratorApp *migrator.MigratorApp
	if err := container.Invoke(func(_m *migrator.MigratorApp) {
		migratorApp = _m
	}); err != nil {
		panic(err)
	}
	return migratorApp
}

func GetTriggerService() *triggerservice.Worker {
	var trigger *triggerservice.Worker
	if err := container.Invoke(func(_s *triggerservice.Worker) {
		trigger = _s
	}); err != nil {
		panic(err)
	}
	return trigger
}
func GetExecutorService() *executorservice.Worker {
	var executor *executorservice.Worker
	if err := container.Invoke(func(_s *executorservice.Worker) {
		executor = _s
	}); err != nil {
		panic(err)
	}
	return executor
}
