package io

import (
	"bufio"
	"io"
	"sync"
	"time"
)

// TimedWriter is a buffered writer that flushes automatically
// when a deadline is set and the deadline is exceeded.
type TimedWriter struct {
	*bufio.Writer
	mutex    sync.Mutex
	deadline time.Duration
	maxQueue int

	queueSize int
	timer     *time.Timer
	prevErr   error
}

func NewTimedWriter(io io.Writer, bufsize int) *TimedWriter {
	return &TimedWriter{
		Writer:   bufio.NewWriterSize(io, bufsize),
		deadline: 1 * time.Millisecond,
		maxQueue: 8,
	}
}

func (w *TimedWriter) SetDeadline(d time.Duration) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.deadline = d
}

func (w *TimedWriter) SetMaxQueue(s int) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.maxQueue = s
}

func (w *TimedWriter) Flush() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.flush_()
}

func (w *TimedWriter) Write(p []byte) (n int, err error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if err := w.prevErr; err != nil {
		w.prevErr = nil
		return 0, err
	}

	n, err = w.Writer.Write(p)
	if err != nil {
		return n, err
	}

	w.queueSize++
	if w.deadline == 0 || w.queueSize >= w.maxQueue {
		return n, w.flush_()
	}

	if w.timer == nil {
		w.timer = time.AfterFunc(w.deadline, func() { w.Flush() })
	}

	return
}

func (w *TimedWriter) flush_() error {
	err := w.Writer.Flush()
	if err != nil {
		w.prevErr = err
	}

	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	w.queueSize = 0

	return err
}
