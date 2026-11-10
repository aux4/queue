package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
)

func buildURL(port, path, token, name string) string {
	u := fmt.Sprintf("http://localhost:%s%s", port, path)
	v := url.Values{}
	if token != "" {
		v.Set("token", token)
	}
	if name != "" {
		v.Set("name", name)
	}
	if len(v) > 0 {
		u += "?" + v.Encode()
	}
	return u
}

func stopServer(args []string) {
	port := getArg(args, 0, "8420")

	pidFile := pidFilePath(port)
	data, err := os.ReadFile(pidFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "server not running on port %s\n", port)
		os.Exit(1)
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid pid file: %s\n", pidFile)
		os.Remove(pidFile)
		os.Exit(1)
	}

	process, err := os.FindProcess(pid)
	if err != nil {
		fmt.Fprintf(os.Stderr, "process %d not found\n", pid)
		os.Remove(pidFile)
		os.Exit(1)
	}

	if err := process.Signal(syscall.SIGTERM); err != nil {
		fmt.Fprintf(os.Stderr, "failed to stop server: %v\n", err)
		os.Remove(pidFile)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stdout, "{\"status\":\"STOPPED\",\"port\":\"%s\",\"pid\":%d}\n", port, pid)
}

func createQueue(args []string) {
	port := getArg(args, 0, "8420")
	token := getArg(args, 1, "")
	name := getArg(args, 2, "")
	wsURL := getArg(args, 3, "")
	webhookURL := getArg(args, 4, "")
	headerCSV := getArg(args, 5, "")
	workQueue := getArg(args, 6, "")
	maxAttempts := getArg(args, 7, "")

	if name == "" {
		fmt.Fprintln(os.Stderr, "queue name is required")
		os.Exit(1)
	}

	u := buildURL(port, "/create", token, name)
	if wsURL != "" {
		u += "&ws=" + url.QueryEscape(wsURL)
	}
	if webhookURL != "" {
		u += "&webhook=" + url.QueryEscape(webhookURL)
	}
	if headerCSV != "" {
		headers := strings.Split(headerCSV, ",")
		for _, h := range headers {
			h = strings.TrimSpace(h)
			if h != "" {
				u += "&header=" + url.QueryEscape(h)
			}
		}
	}
	if workQueue == "true" {
		u += "&workQueue=true"
	}
	if maxAttempts != "" {
		u += "&maxAttempts=" + url.QueryEscape(maxAttempts)
	}

	resp, err := http.Post(u, "application/json", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		fmt.Fprintf(os.Stderr, "%s\n", body)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "%s", body)
}

func deleteQueue(args []string) {
	port := getArg(args, 0, "8420")
	token := getArg(args, 1, "")
	name := getArg(args, 2, "")

	if name == "" {
		fmt.Fprintln(os.Stderr, "queue name is required")
		os.Exit(1)
	}

	resp, err := http.Post(buildURL(port, "/delete", token, name), "application/json", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		fmt.Fprintf(os.Stderr, "%s\n", body)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "%s", body)
}

func listQueues(args []string) {
	port := getArg(args, 0, "8420")
	token := getArg(args, 1, "")

	resp, err := http.Get(buildURL(port, "/list", token, ""))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		fmt.Fprintf(os.Stderr, "%s\n", body)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "%s", body)
}

func sendMessages(args []string) {
	port := getArg(args, 0, "8420")
	token := getArg(args, 1, "")
	name := getArg(args, 2, "")
	group := getArg(args, 3, "")
	id := getArg(args, 4, "")

	if name == "" {
		fmt.Fprintln(os.Stderr, "queue name is required")
		os.Exit(1)
	}

	endpoint := buildURL(port, "/send", token, name)
	if group != "" {
		endpoint += "&group=" + url.QueryEscape(group)
	}
	if id != "" {
		endpoint += "&id=" + url.QueryEscape(id)
	}
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	sent := 0
	errors := 0

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		if !json.Valid(line) {
			fmt.Fprintf(os.Stderr, "skipping invalid JSON: %s\n", line)
			errors++
			continue
		}

		resp, err := http.Post(endpoint, "application/json", bytes.NewReader(line))
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			errors++
			continue
		}
		resp.Body.Close()

		if resp.StatusCode >= 400 {
			fmt.Fprintf(os.Stderr, "error sending message: %s\n", resp.Status)
			errors++
			continue
		}
		sent++
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "read error: %v\n", err)
	}

	result := map[string]int{"sent": sent, "errors": errors}
	json.NewEncoder(os.Stdout).Encode(result)
}

func receiveMessages(args []string) {
	port := getArg(args, 0, "8420")
	token := getArg(args, 1, "")
	name := getArg(args, 2, "")
	limitStr := getArg(args, 3, "-1")

	if name == "" {
		fmt.Fprintln(os.Stderr, "queue name is required")
		os.Exit(1)
	}

	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid limit: %s\n", limitStr)
		os.Exit(1)
	}

	resp, err := http.Get(buildURL(port, "/receive", token, name))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "%s\n", body)
		os.Exit(1)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)
	count := 0
	for scanner.Scan() {
		fmt.Fprintln(os.Stdout, scanner.Text())
		count++
		if limit > 0 && count >= limit {
			return
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "connection error: %v\n", err)
		os.Exit(1)
	}
}

// leaseMessage leases the next eligible message from a work queue.
// args: port, token, name, visibility, group (reserved for future per-call
// narrowing; the server currently decides group eligibility per message).
func leaseMessage(args []string) {
	port := getArg(args, 0, "8420")
	token := getArg(args, 1, "")
	name := getArg(args, 2, "")
	visibility := getArg(args, 3, "")

	if name == "" {
		fmt.Fprintln(os.Stderr, "queue name is required")
		os.Exit(1)
	}
	if visibility == "" {
		fmt.Fprintln(os.Stderr, "visibility (seconds) is required")
		os.Exit(1)
	}

	u := buildURL(port, "/lease", token, name)
	u += "&visibility=" + url.QueryEscape(visibility)

	resp, err := http.Post(u, "application/json", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		fmt.Fprintln(os.Stdout, "{}")
		os.Exit(4)
	}
	if resp.StatusCode >= 400 {
		fmt.Fprintf(os.Stderr, "%s\n", body)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "%s", body)
}

// ackMessage acknowledges (completes) a leased message.
// args: port, token, name, leaseId
func ackMessage(args []string) {
	port := getArg(args, 0, "8420")
	token := getArg(args, 1, "")
	name := getArg(args, 2, "")
	leaseID := getArg(args, 3, "")

	if name == "" || leaseID == "" {
		fmt.Fprintln(os.Stderr, "queue name and leaseId are required")
		os.Exit(1)
	}

	u := buildURL(port, "/ack", token, name)
	u += "&leaseId=" + url.QueryEscape(leaseID)

	resp, err := http.Post(u, "application/json", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		fmt.Fprintf(os.Stderr, "%s\n", body)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "%s", body)
}

// nackMessage releases a leased message for immediate redelivery, bumping
// its attempts counter (and moving it to the dead-letter queue once
// maxAttempts is reached).
// args: port, token, name, leaseId
func nackMessage(args []string) {
	port := getArg(args, 0, "8420")
	token := getArg(args, 1, "")
	name := getArg(args, 2, "")
	leaseID := getArg(args, 3, "")

	if name == "" || leaseID == "" {
		fmt.Fprintln(os.Stderr, "queue name and leaseId are required")
		os.Exit(1)
	}

	u := buildURL(port, "/nack", token, name)
	u += "&leaseId=" + url.QueryEscape(leaseID)

	resp, err := http.Post(u, "application/json", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		fmt.Fprintf(os.Stderr, "%s\n", body)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "%s", body)
}

// listDLQ lists the dead-letter messages for a work queue.
// args: port, token, name
func listDLQ(args []string) {
	port := getArg(args, 0, "8420")
	token := getArg(args, 1, "")
	name := getArg(args, 2, "")

	if name == "" {
		fmt.Fprintln(os.Stderr, "queue name is required")
		os.Exit(1)
	}

	resp, err := http.Get(buildURL(port, "/dlq", token, name))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		fmt.Fprintf(os.Stderr, "%s\n", body)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "%s", body)
}
