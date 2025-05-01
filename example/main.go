package main

import "github.com/tompston/syro"

func main() {
	logger := syro.NewConsoleLogger(nil).SetSource("my-event").SetEvent("my-source")
	logger.Info("Hello World")
}
