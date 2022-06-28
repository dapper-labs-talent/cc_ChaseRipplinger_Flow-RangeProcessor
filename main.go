package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/ripply/flowrangeprocessor/pkg/processor"
	"github.com/ripply/flowrangeprocessor/pkg/types"
)

var SIZE uint64 = 50
var NUMBER int = 5

func main() {
	rand.Seed(time.Now().UnixNano())

	ctx, done := context.WithCancel(context.Background())
	defer done()

	p := processor.NewRangeResponseProcessor(ctx, SIZE, NUMBER)
	min, max := p.GetActiveRange()
	fmt.Printf("min: %d, max: %d\n", min, max)

	for i := 0; i < 40; i++ {
		go processRange(ctx, p)
	}

	t := time.After(10 * time.Second)
	complete := false
	for !complete {
		select {
		case <-time.After(50 * time.Millisecond):
		case <-t:
			complete = true
		}

		min, max := p.GetActiveRange()
		fmt.Printf("min: %d, max: %d\n", min, max)
	}

}

func processRange(ctx context.Context, p processor.RangeResponseProcessor) {
	sizeMin := uint64(0)
	sizeMax := uint64(SIZE * 1)
	for {
		// get current active range
		min, max := p.GetActiveRange()
		// randomly adjust the number of blocks we will process
		size := rand.Int63n(int64(sizeMax-sizeMin)) + int64(sizeMin)
		// randmoly adjust the block we start from
		from := rand.Int63n(int64(max+SIZE-min)) + int64(min-SIZE)

		blocks := make([]types.Block, size)
		for i := int64(0); i < size; i++ {
			blocks[i] = types.Block(fmt.Sprintf("block: %d", from+i))
		}

		// fmt.Printf("ProcessRange(%d, %d)\n", from, len(blocks))
		p.ProcessRange(uint64(from), blocks)
	}
}
