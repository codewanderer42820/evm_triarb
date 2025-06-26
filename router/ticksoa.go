package router

import "main/bucketqueue"

// tickSoA now contains only the queue plus the anchor-pair tick.
type tickSoA struct {
	Queue      bucketqueue.Queue // SPSC bucket queue
	CommonTick float64           // last log₂ tick of this pair
}
