package streamaggr

import (
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
)

// sumSamplesAggrState calculates output=sum_samples, e.g. the sum over input samples.
type sumSamplesAggrState struct {
	m sync.Map

	// Time series state is dropped if no new samples are received during stalenessMsecs.
	//
	// Aslo, the first sample per each new series is ignored during stalenessMsecs even if keepFirstSample is set.
	stalenessMsecs int64
}

type sumSamplesStateValue struct {
	mu             sync.Mutex
	sum            []*float64
	deleted        bool
	deleteDeadline int64
}

func newSumSamplesAggrState(stalenessInterval time.Duration) *sumSamplesAggrState {
	stalenessMsecs := durationToMsecs(stalenessInterval)
	return &sumSamplesAggrState{
		stalenessMsecs: stalenessMsecs,
	}
}

func (as *sumSamplesAggrState) pushSamples(windows [][]pushSample) {
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
				v = &sumSamplesStateValue{
					sum: make([]*float64, len(windows)),
				}
				vNew, loaded := as.m.LoadOrStore(outputKey, v)
				if loaded {
					// Use the entry created by a concurrent goroutine.
					v = vNew
				}
			}
			sv := v.(*sumSamplesStateValue)
			sv.mu.Lock()
			deleted := sv.deleted
			if !deleted {
				if sv.sum[w] == nil {
					sv.sum[w] = &s.value
				} else {
					*sv.sum[w] += s.value
				}
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

func (as *sumSamplesAggrState) flushState(ctx *flushCtx, flushTimestamp int64, w int) {
	m := &as.m
	m.Range(func(k, v interface{}) bool {
		sv := v.(*sumSamplesStateValue)
		sv.mu.Lock()
		deleted := flushTimestamp > sv.deleteDeadline
		state := sv.sum[w]
		if deleted {
			// Mark the current entry as deleted
			sv.deleted = deleted
		} else if state == nil {
			sv.mu.Unlock()
			return true
		}
		sv.sum[w] = nil
		sv.mu.Unlock()
		key := k.(string)
		ctx.appendSeries(key, "sum_samples", flushTimestamp, *state)
		if deleted {
			m.Delete(k)
		}
		return true
	})
}
