package streamaggr

import (
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/VictoriaMetrics/metrics"
)

// histogramBucketAggrState calculates output=histogram_bucket, e.g. VictoriaMetrics histogram over input samples.
type histogramBucketAggrState struct {
	m sync.Map

	stalenessMsecs int64
}

type histogramBucketStateValue struct {
	mu             sync.Mutex
	h              []*metrics.Histogram
	deleteDeadline int64
	deleted        bool
}

func newHistogramBucketAggrState(stalenessInterval time.Duration) *histogramBucketAggrState {
	stalenessMsecs := durationToMsecs(stalenessInterval)
	return &histogramBucketAggrState{
		stalenessMsecs: stalenessMsecs,
	}
}

func (as *histogramBucketAggrState) pushSamples(windows [][]pushSample) {
	currentTime := fasttime.UnixMilli()
	deleteDeadline := currentTime + as.stalenessMsecs
	for w, samples := range windows {
		for i := range samples {
			s := &samples[i]
			outputKey := getOutputKey(s.key)

		again:
			v, ok := as.m.Load(outputKey)
			if !ok {
				// The entry is missing in the map. Try creating it.
				v = &histogramBucketStateValue{
					h: make([]*metrics.Histogram, len(windows)),
				}
				if vNew, loaded := as.m.LoadOrStore(outputKey, v); loaded {
					v = vNew
				}
			}
			sv := v.(*histogramBucketStateValue)
			sv.mu.Lock()
			deleted := sv.deleted
			if !deleted {
				if sv.h[w] == nil {
					sv.h[w] = &metrics.Histogram{}
				}
				sv.h[w].Update(s.value)
				sv.deleteDeadline = deleteDeadline
			}
			sv.mu.Unlock()
			if deleted {
				// The entry has been deleted by the concurrent call to flushState
				// Try obtaining and updating the entry again.
				goto again
			}
		}
	}
}

func (as *histogramBucketAggrState) flushState(ctx *flushCtx, flushTimestamp int64, w int) {
	m := &as.m
	m.Range(func(k, v interface{}) bool {
		sv := v.(*histogramBucketStateValue)
		sv.mu.Lock()
		deleted := flushTimestamp > sv.deleteDeadline
		state := sv.h[w]
		if deleted {
			// Mark the current entry as deleted
			sv.deleted = deleted
		} else if state == nil {
			sv.mu.Unlock()
			return true
		}
		sv.h[w] = nil
		sv.mu.Unlock()
		key := k.(string)
		state.VisitNonZeroBuckets(func(vmrange string, count uint64) {
			ctx.appendSeriesWithExtraLabel(key, "histogram_bucket", flushTimestamp, float64(count), "vmrange", vmrange)
		})
		if deleted {
			m.Delete(k)
		}
		return true
	})
}

func durationToMsecs(d time.Duration) int64 {
	if d < 0 {
		return 0
	}
	return d.Milliseconds()
}
