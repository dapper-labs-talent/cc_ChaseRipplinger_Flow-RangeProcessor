package processor

import (
	"context"
	"sync"

	"github.com/ripply/flowrangeprocessor/pkg/types"
)

type RangeResponseProcessor interface {
	ProcessRange(startHeight uint64, blocks []types.Block)
	GetActiveRange() (minHeight uint64, maxHeight uint64)
}

func NewRangeResponseProcessor(
	ctx context.Context,
	size uint64,
	numberOfResponses int,
) RangeResponseProcessor {
	processor := &rangeResponseProcessor{
		ctx:           ctx,
		height:        0,
		size:          size,
		responses:     numberOfResponses,
		buckets:       make([]*int, size),
		bucketMutexes: make([]sync.RWMutex, size),
		bucketOffset:  0,
	}

	processor.initialize()

	return processor
}

type rangeResponseProcessor struct {
	heightMutex sync.RWMutex
	ctx         context.Context

	height    uint64
	size      uint64
	responses int

	buckets       []*int
	bucketMutexes []sync.RWMutex
	bucketOffset  int
	offsetMutex   sync.RWMutex
}

func (p *rangeResponseProcessor) initialize() {
	p.heightMutex.Lock()
	defer p.heightMutex.Unlock()
	for i := uint64(0); i < p.size; i++ {
		initialValue := 0
		p.buckets[i] = &initialValue
	}
}

func (p *rangeResponseProcessor) ProcessRange(startHeight uint64, blocks []types.Block) {
	blockFilled := false
	// grab read lock on height while we are processing a range
	p.heightMutex.RLock()
	min, max := p.getActiveRange()

	blockCount := len(blocks)
	if uint64(blockCount) > p.size {
		blockCount = int(p.size)
	}
	if startHeight > max || startHeight+uint64(blockCount) < min {
		// everything is out of the active range
		p.heightMutex.RUnlock()
		return
	}

	p.offsetMutex.RLock()
	startProcessingFromOffset := startHeight - min
	for i := startProcessingFromOffset; i < uint64(startProcessingFromOffset+uint64(blockCount)); i++ {
		bucketIndex := (uint64(p.bucketOffset) + i) % p.size
		p.bucketMutexes[bucketIndex].Lock()
		bucketValue := p.buckets[bucketIndex]
		*bucketValue += 1
		if *bucketValue >= p.responses {
			// reached number of responses for this block
			blockFilled = true
		}
		p.bucketMutexes[bucketIndex].Unlock()
	}
	p.offsetMutex.RUnlock()
	p.heightMutex.RUnlock()

	if blockFilled {
		completedBlocks := 0
		p.heightMutex.Lock()
		p.offsetMutex.Lock()
		// start from bucketOffset and for continuous each bucket that is filled
		// bump height and reset to zero
		blockFilled := true
		for i := uint64(0); i < p.size && blockFilled; i++ {
			index := (uint64(p.bucketOffset) + i) % p.size
			p.bucketMutexes[index].Lock()
			if *p.buckets[index] > p.responses {
				*p.buckets[index] = 0
				completedBlocks++
			} else {
				blockFilled = false
			}
			p.bucketMutexes[index].Unlock()
		}
		if completedBlocks != 0 {
			p.bucketOffset = (p.bucketOffset + completedBlocks) % int(p.size)
			p.height += uint64(completedBlocks)
		}
		p.offsetMutex.Unlock()
		p.heightMutex.Unlock()
	}
}

func (p *rangeResponseProcessor) GetActiveRange() (minHeight uint64, maxHeight uint64) {
	p.heightMutex.RLock()
	defer p.heightMutex.RUnlock()
	return p.getActiveRange()
}

func (p *rangeResponseProcessor) getActiveRange() (minHeight uint64, maxHeight uint64) {
	return p.height, p.height + p.size - 1
}
