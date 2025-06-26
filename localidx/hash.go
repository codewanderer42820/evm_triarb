// localidx/hash.go — fixed-size Robin-Hood map.

package localidx

type Hash struct {
	keys, vals []uint32
	mask       uint32
}

func nextPow2(n int) uint32 {
	s := uint32(1)
	for s < uint32(n) {
		s <<= 1
	}
	return s
}

func New(capacity int) Hash {
	sz := nextPow2(capacity * 2) // load ≤ 0.5
	return Hash{
		keys: make([]uint32, sz),
		vals: make([]uint32, sz),
		mask: sz - 1,
	}
}

func probeDist(key, slot, mask uint32) uint32 { return (slot + mask + 1 - (key & mask)) & mask }

func (h Hash) Put(key, val uint32) uint32 {
	i := key & h.mask
	dist := uint32(0)
	for {
		k := h.keys[i]
		if k == 0 {
			h.keys[i], h.vals[i] = key, val
			return val
		}
		if k == key {
			return h.vals[i]
		}
		if probeDist(k, i, h.mask) < dist {
			key, h.keys[i] = h.keys[i], key
			val, h.vals[i] = h.vals[i], val
			dist = probeDist(key, i, h.mask)
		}
		i, dist = (i+1)&h.mask, dist+1
	}
}

func (h Hash) Get(key uint32) (uint32, bool) {
	i := key & h.mask
	dist := uint32(0)
	for {
		k := h.keys[i]
		if k == 0 {
			return 0, false
		}
		if k == key {
			return h.vals[i], true
		}
		if probeDist(k, i, h.mask) < dist {
			return 0, false
		}
		i, dist = (i+1)&h.mask, dist+1
	}
}
