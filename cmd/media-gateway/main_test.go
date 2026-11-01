package main

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestSupervisorPipeEOFTriggersShutdown(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchSupervisorPipe(r, cancel)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("stdin EOF did not stop gateway")
	}
}
