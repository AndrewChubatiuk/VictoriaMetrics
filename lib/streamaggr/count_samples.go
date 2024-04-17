package streamaggr

import (
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
)

// countSamplesAggrState calculates output=count_samples, e.g. the count of input samples.
type countSamplesAggrState struct {
	m sync.Map

	// Time series state is dropped if no new samples are received during stalenessMsecs.
	//
	// Aslo, the first sample per each new series is ignored during stalenessMsecs even if keepFirstSample is set.
	stalenessMsecs int64
}

type countSamplesStateValue struct {
	mu             sync.Mutex
	n              []*uint64
	deleted        bool
	deleteDeadline int64
}

func newCountSamplesAggrState(stalenessInterval time.Duration) *countSamplesAggrState {
	stalenessMsecs := durationToMsecs(stalenessInterval)
	return &countSamplesAggrState{
		stalenessMsecs: stalenessMsecs,
	}
}

func (as *countSamplesAggrState) pushSamples(windows [][]pushSample) {
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
				v = &countSamplesStateValue{
					n: make([]*uint64, len(windows)),
				}
				if vNew, loaded := as.m.LoadOrStore(outputKey, v); loaded {
					v = vNew
				}
			}
			sv := v.(*countSamplesStateValue)
			sv.mu.Lock()
			deleted := sv.deleted
			if !deleted {
				if sv.n[w] == nil {
					sv.n[w] = new(uint64)
				}
				*sv.n[w]++
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

func (as *countSamplesAggrState) flushState(ctx *flushCtx, flushTimestamp int64, w int) {
	m := &as.m
	m.Range(func(k, v interface{}) bool {
		sv := v.(*countSamplesStateValue)
		sv.mu.Lock()
		deleted := flushTimestamp > sv.deleteDeadline
		state := sv.n[w]
		if deleted {
			// Mark the current entry as deleted
			sv.deleted = deleted
		} else if state == nil {
			sv.mu.Unlock()
			return true
		}
		sv.n[w] = nil
		sv.mu.Unlock()
		key := k.(string)
		ctx.appendSeries(key, "count_samples", flushTimestamp, float64(*state))
		if deleted {
			m.Delete(k)
		}
		return true
	})
}
