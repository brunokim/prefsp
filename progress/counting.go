package progress

import (
	"fmt"
	"log"
	"math"
	"time"
)

type WriterLogger struct {
	ch     chan int
	done   chan struct{}
	ticker *time.Ticker
}

func NewWriterLogger(period time.Duration) *WriterLogger {
	l := new(WriterLogger)
	l.ch = make(chan int, 1)
	l.done = make(chan struct{})
	l.ticker = time.NewTicker(period)
	go func() {
		var numFiles, numBytes int
		start := time.Now()
		tick := func() {
			elapsed := time.Since(start)
			fileRate := float64(numFiles) / elapsed.Minutes()
			byteRate := float64(numBytes) / elapsed.Seconds()
			log.Printf("Wrote %5d files (%.2f files/min) with %s (%s/s)", numFiles, fileRate, byteUnit(float64(numBytes)), byteUnit(byteRate))
		}
		for {
			select {
			case n := <-l.ch:
				numFiles++
				numBytes += n
			case <-l.ticker.C:
				tick()
			case <-l.done:
				// Final write
				tick()
			}
		}
	}()
	return l
}

func (l *WriterLogger) Wrote(n int) {
	l.ch <- n
}

func (l *WriterLogger) Stop() {
	l.ticker.Stop()
	l.done <- struct{}{}
}

func byteUnit(x float64) string {
	powerOf2 := math.Log2(x)
	if powerOf2 < 9.5 {
		return fmt.Sprintf("%.2f B", x)
	}
	if powerOf2 < 19.5 {
		return fmt.Sprintf("%.2f KiB", x/(1<<10))
	}
	if powerOf2 < 29.5 {
		return fmt.Sprintf("%.2f MiB", x/(1<<20))
	}
	if powerOf2 < 39.5 {
		return fmt.Sprintf("%.2f GiB", x/(1<<30))
	}
	return fmt.Sprintf("%.2f TiB", x/(1<<40))
}
