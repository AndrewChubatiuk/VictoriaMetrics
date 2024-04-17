package streamaggr

import (
	"sync"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/cespare/xxhash/v2"
)

const dedupAggrShardsCount = 128

type dedupAggr struct {
	shards []dedupAggrShard
}

type dedupAggrShard struct {
	dedupAggrShardNopad

	// The padding prevents false sharing on widespread platforms with
	// 128 mod (cache line size) = 0 .
	_ [128 - unsafe.Sizeof(dedupAggrShardNopad{})%128]byte
}

type dedupAggrShardNopad struct {
	mu sync.Mutex
	m  []map[string]*dedupAggrSample
}

type dedupAggrSample struct {
	value     float64
	timestamp int64
}

func newDedupAggr() *dedupAggr {
	shards := make([]dedupAggrShard, dedupAggrShardsCount)
	return &dedupAggr{
		shards: shards,
	}
}

func (da *dedupAggr) sizeBytes() uint64 {
	n := uint64(unsafe.Sizeof(*da))
	for i := range da.shards {
		n += da.shards[i].sizeBytes()
	}
	return n
}

func (da *dedupAggr) itemsCount() uint64 {
	n := uint64(0)
	for i := range da.shards {
		n += da.shards[i].itemsCount()
	}
	return n
}

func (das *dedupAggrShard) sizeBytes() uint64 {
	das.mu.Lock()
	n := uint64(unsafe.Sizeof(*das))
	for _, s := range das.m {
		for k, m := range s {
			n += uint64(len(k)) + uint64(unsafe.Sizeof(k)+unsafe.Sizeof(m))
		}
	}
	das.mu.Unlock()
	return n
}

func (das *dedupAggrShard) itemsCount() uint64 {
	das.mu.Lock()
	var n uint64
	for _, m := range das.m {
		n += uint64(len(m))
	}
	das.mu.Unlock()
	return n
}

func (da *dedupAggr) pushSamples(windows [][]pushSample) {
	pss := getPerShardSamples()
	shards := pss.shards
	for w, samples := range windows {
		for _, sample := range samples {
			h := xxhash.Sum64(bytesutil.ToUnsafeBytes(sample.key))
			idx := h % uint64(len(shards))
			shards[idx] = append(shards[idx], sample)
		}
		for i, shardSamples := range shards {
			if len(shardSamples) == 0 {
				continue
			}
			da.shards[i].pushSamples(shardSamples, len(windows), w)
		}
	}
	putPerShardSamples(pss)
}

func getDedupFlushCtx(w int) *dedupFlushCtx {
	v := dedupFlushCtxPool.Get()
	if v == nil {
		return &dedupFlushCtx{
			samples: make([][]pushSample, w),
		}
	}
	ctx := v.(*dedupFlushCtx)
	if len(ctx.samples) != w {
		samplesCap := cap(ctx.samples)
		if cap(ctx.samples) < w {
			ctx.samples = append(ctx.samples[:samplesCap], make([][]pushSample, w-samplesCap)...)
		} else {
			ctx.samples = ctx.samples[:w]
		}
	}
	return v.(*dedupFlushCtx)
}

func putDedupFlushCtx(ctx *dedupFlushCtx) {
	ctx.reset()
	dedupFlushCtxPool.Put(ctx)
}

var dedupFlushCtxPool sync.Pool

type dedupFlushCtx struct {
	samples [][]pushSample
}

func (ctx *dedupFlushCtx) reset() {
	for s := range ctx.samples {
		ctx.samples[s] = ctx.samples[s][:0]
	}
}

func (da *dedupAggr) flush(f func(samples [][]pushSample), dedupIndex, flushIndex int) {
	var wg sync.WaitGroup
	for i := range da.shards {
		flushConcurrencyCh <- struct{}{}
		wg.Add(1)
		go func(shard *dedupAggrShard) {
			defer func() {
				<-flushConcurrencyCh
				wg.Done()
			}()

			var ctx dedupFlushCtx
			shard.flush(&ctx, f, dedupIndex, flushIndex)
			putDedupFlushCtx(&ctx)
		}(&da.shards[i])
	}
	wg.Wait()
}

type perShardSamples struct {
	shards [][]pushSample
}

func (pss *perShardSamples) reset() {
	shards := pss.shards
	for i, shardSamples := range shards {
		if len(shardSamples) > 0 {
			clear(shardSamples)
			shards[i] = shardSamples[:0]
		}
	}
}

func getPerShardSamples() *perShardSamples {
	v := perShardSamplesPool.Get()
	if v == nil {
		return &perShardSamples{
			shards: make([][]pushSample, dedupAggrShardsCount),
		}
	}
	return v.(*perShardSamples)
}

func putPerShardSamples(pss *perShardSamples) {
	pss.reset()
	perShardSamplesPool.Put(pss)
}

var perShardSamplesPool sync.Pool

func (das *dedupAggrShard) pushSamples(samples []pushSample, windowsCount, w int) {
	das.mu.Lock()
	defer das.mu.Unlock()

	if das.m == nil {
		das.m = make([]map[string]*dedupAggrSample, windowsCount)
	}
	for _, sample := range samples {
		if das.m[w] == nil {
			das.m[w] = make(map[string]*dedupAggrSample)
		}
		if s, ok := das.m[w][sample.key]; !ok {
			das.m[w][sample.key] = &dedupAggrSample{
				value:     sample.value,
				timestamp: sample.timestamp,
			}
		} else if sample.timestamp > s.timestamp || (sample.timestamp == s.timestamp && sample.value > s.value) {
			// Update the existing value according to logic described at https://docs.victoriametrics.com/#deduplication
			das.m[w][sample.key] = &dedupAggrSample{
				value:     sample.value,
				timestamp: sample.timestamp,
			}
		}
	}
}

func (das *dedupAggrShard) flush(ctx *dedupFlushCtx, f func(samples [][]pushSample), dedupIndex, flushIndex int) {
	das.mu.Lock()
	if len(das.m) == 0 {
		das.mu.Unlock()
		return
	}
	ctx = getDedupFlushCtx(len(das.m))
	state := das.m[dedupIndex]
	if state != nil {
		das.m[dedupIndex] = nil
		das.mu.Unlock()
		for key, s := range state {
			ctx.samples[flushIndex] = append(ctx.samples[flushIndex], pushSample{
				key:       key,
				value:     s.value,
				timestamp: s.timestamp,
			})
			// Limit the number of samples per each flush in order to limit memory usage.
			if len(ctx.samples[flushIndex]) >= 100_000 {
				f(ctx.samples)
				clear(ctx.samples)
			}
		}
	} else {
		das.mu.Unlock()
	}

	f(ctx.samples)
}
