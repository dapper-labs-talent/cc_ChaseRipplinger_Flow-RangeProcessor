package processor

import (
	"context"
	"fmt"
	"testing"

	"github.com/ripply/flowrangeprocessor/pkg/types"
)

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if a != b {
		t.Fatalf("%s != %s", a, b)
	}
}

func TestProcessRange(t *testing.T) {
	ctx, done := context.WithCancel(context.Background())
	defer done()

	blocks := make([]types.Block, 10)
	for i := 0; i < len(blocks); i++ {
		blocks[i] = types.Block(fmt.Sprintf("Block: %d", i))
	}

	p := NewRangeResponseProcessor(ctx, 5, 3)
	min, max := p.GetActiveRange()
	assertEqual(t, min, uint64(0))
	assertEqual(t, max, uint64(4))

	// call 3 times
	p.ProcessRange(0, blocks[0:1])
	min, max = p.GetActiveRange()
	assertEqual(t, min, uint64(0))
	assertEqual(t, max, uint64(4))

	p.ProcessRange(0, blocks[0:1])
	min, max = p.GetActiveRange()
	assertEqual(t, min, uint64(0))
	assertEqual(t, max, uint64(4))

	p.ProcessRange(0, blocks[0:1])
	// verify minimum increased
	min, max = p.GetActiveRange()
	assertEqual(t, min, uint64(1))
	assertEqual(t, max, uint64(5))

	for i := 0; i < 10; i++ {
		// test blocks out of range, make sure nothing happens
		p.ProcessRange(0, blocks[0:1])
		p.ProcessRange(100, blocks[0:1])
		min, max = p.GetActiveRange()
		assertEqual(t, min, uint64(1))
		assertEqual(t, max, uint64(5))
	}

	for i := 0; i < 3; i++ {
		// test old block but that has valid blocks we are looking for after
		min, max = p.GetActiveRange()
		assertEqual(t, min, uint64(1))
		assertEqual(t, max, uint64(5))
		p.ProcessRange(0, blocks[0:2])
	}

	// verify minimum increased
	min, max = p.GetActiveRange()
	assertEqual(t, min, uint64(2))
	assertEqual(t, max, uint64(6))
}
