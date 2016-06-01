package main

import "time"
import "fmt"

import "github.com/RubyGarage/odbcimporter"

func main() {
	credentials := map[string]string{}

	client, err := odbcimporter.NewClient(credentials)
	eventsCh := make(chan odbcimporter.Event)
	go client.Events(eventsCh)

	for event := range eventsCh {
		// do something with event
	}
}
