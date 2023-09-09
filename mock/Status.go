package main

type Status string

const (
	Failure Status = "FAILURE"
	Retry   Status = "RETRY"
	Skip    Status = "SKIP"
	Success Status = "SUCCESS"
)
