package main

import (
	"timerTask/app"
)

func main() {
	trigger := app.GetTriggerService()
	trigger.Work()
}
