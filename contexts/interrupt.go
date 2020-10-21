package contexts

import (
	"context"
	"os"
	"os/signal"
)

const (
	ExitCodeInterrupt = 2
)

func WithInterrupt(ctx context.Context) (context.Context, context.CancelFunc) {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-interrupt:
			cancel()
		case <-ctx.Done():
		}
		// Second interrupt makes for an abnormal exit.
		<-interrupt
		os.Exit(ExitCodeInterrupt)
	}()
	return ctx, func() {
		signal.Stop(interrupt)
		cancel()
	}
}
