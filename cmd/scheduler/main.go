package main

import (
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"timerTask/app"
)

func main() {
	migratorApp := app.GetMigratorApp()
	//schedulerApp := app.GetSchedulerApp()
	//webServer := app.GetWebServer()

	migratorApp.Start() // 开启定时任务打点器
	//schedulerApp.Start() // 开启定时任务调度器
	//defer schedulerApp.Stop()

	//webServer.Start() // 开启HTTP监听

	// 支持 pprof
	go func() {
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {})
		_ = http.ListenAndServe(":9999", nil)
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT)
	<-quit
}
