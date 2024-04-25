package main

import (
	"context"
	"timerTask/app"
)

func main() {
	executor := app.GetExecutorService()
	executor.Start(context.Background())

	executor.Work()
}
