package main

import (
	"log"
	"math/rand"
	"time"

	"github.com/m1keil/microbatching"
)

func Process(batch []int) error {
	log.Println(batch)
	return nil
}

func main() {
	batcher := microbatching.NewBatcher(5, time.Second, Process)

	for i := 0; i < 100; i++ {
		err := batcher.Submit(i)
		if err != microbatching.OK {
			log.Println("unable to submit", i)
		}
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(500)))
	}
	batcher.Shutdown()
}
