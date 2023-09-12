package microbatching

import (
	"log"
	"sync"
	"time"
)

type JobResult int

const (
	OK   = 0
	BUSY = 1
)

type BatchProcessor func([]int) error

type Batcher struct {
	sync.RWMutex
	incoming chan int
	done     chan struct{}
	buffer   []int
	outgoing BatchProcessor
	freq     *time.Ticker
}

func NewBatcher(size int, freq time.Duration, processor BatchProcessor) *Batcher {
	b := Batcher{
		incoming: make(chan int, 1),
		done:     make(chan struct{}),
		buffer:   make([]int, 0, size),
		outgoing: processor,
		freq:     time.NewTicker(freq),
	}

	go func() {
		for {
			select {
			case incoming := <-b.incoming:
				b.Lock()
				b.buffer = append(b.buffer, incoming)
				b.Unlock()
				if len(b.buffer) >= size {
					log.Println("flushing due to limit")
					b.flush()
					b.freq.Reset(freq)
				}
			case <-b.freq.C:
				log.Println("flushing due to timeout")
				b.flush()
			case <-b.done:
				log.Println("flushing due to shutdown")
				close(b.incoming)
				b.freq.Stop()
				b.flush()
				close(b.done)
				return
			}
		}
	}()

	return &b
}

func (b *Batcher) flush() {
	if len(b.buffer) == 0 {
		return
	}

	b.Lock()
	defer b.Unlock()
	if err := b.outgoing(b.buffer); err != nil {
		log.Println("error submitting batch", err)
	}

	b.buffer = b.buffer[:0]
}

func (b *Batcher) Submit(payload int) JobResult {
	select {
	case b.incoming <- payload:
		return OK
	default:
		return BUSY
	}
}

func (b *Batcher) Shutdown() {
	b.done <- struct{}{}
	<-b.done
}
