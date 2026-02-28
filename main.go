package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: aux4-queue <command> [args...]")
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "start":
		startServer(args)
	case "stop":
		stopServer(args)
	case "create":
		createQueue(args)
	case "delete":
		deleteQueue(args)
	case "list":
		listQueues(args)
	case "send":
		sendMessages(args)
	case "receive":
		receiveMessages(args)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		os.Exit(1)
	}
}

func getArg(args []string, index int, defaultValue string) string {
	if index < len(args) && args[index] != "" {
		return args[index]
	}
	return defaultValue
}
