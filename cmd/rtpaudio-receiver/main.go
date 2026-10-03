// Command rtpaudio-receiver receives fixed-profile RTP PCM audio on UDP and
// writes one WAV and one JSON missing report per source when stopped with
// SIGINT or SIGTERM.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"rtpaudio"
)

func main() {
	addr := flag.String("addr", ":5004", "UDP listen address")
	outDir := flag.String("out", "output", "directory for WAV and missing report files")
	queue := flag.Int("queue", 128, "maximum packets queued per UDP source")
	window := flag.Uint64("window", 32, "reorder window in 20 ms frames")
	flag.Parse()

	receiver := rtpaudio.NewReceiver(nil, rtpaudio.Config{
		QueueCapacity: *queue,
		ReorderWindow: *window,
	})
	server, err := rtpaudio.ListenUDP(*addr, receiver)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}
	defer server.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runErr := make(chan error, 1)
	go func() { runErr <- server.Serve(ctx) }()
	go receiver.Run(ctx)

	log.Printf("RTP receiver listening on %s; press Ctrl-C to export to %s", server.LocalAddr(), *outDir)
	<-ctx.Done()
	log.Printf("shutting down and exporting output records")
	for _, source := range receiver.Sources() {
		if err := receiver.StopSource(source, receiver.ClockNow()); err != nil {
			log.Printf("stop %s: %v", source, err)
			continue
		}
		generations, err := receiver.Generations(source)
		if err != nil {
			log.Printf("list generations for %s: %v", source, err)
			continue
		}
		for _, generation := range generations {
			wav, report, err := receiver.ExportGeneration(*outDir, source, generation)
			if err != nil {
				log.Printf("export %s generation %d: %v", source, generation, err)
				continue
			}
			log.Printf("exported %s and %s", wav, report)
		}
	}
	if err := <-runErr; err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
