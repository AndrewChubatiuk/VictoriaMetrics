package streamaggr

import (
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
)

// minAggrState calculates output=min, e.g. the minimum value over input samples.
type minAggrState struct {
	m sync.Map

	// Time series state is dropped if no new samples are received during stalenessMsecs.
	//
	// Aslo, the first sample per each new series is ignored during stalenessMsecs even if keepFirstSample is set.
	stalenessMsecs int64
}

type minStateValue struct {
	mu             sync.Mutex
	min            []*float64
	deleted        bool
	deleteDeadline int64
}

func newMinAggrState(stalenessInterval time.Duration) *minAggrState {
	stalenessMsecs := durationToMsecs(stalenessInterval)
	return &minAggrState{
		stalenessMsecs: stalenessMsecs,
	}
}

func (as *minAggrState) pushSamples(windows [][]pushSample) {
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
				v = &minStateValue{
					min: make([]*float64, len(windows)),
				}
				if vNew, loaded := as.m.LoadOrStore(outputKey, v); loaded {
					v = vNew
				}
			}
			sv := v.(*minStateValue)
			sv.mu.Lock()
			deleted := sv.deleted
			if !deleted {
				if sv.min[w] == nil {
					sv.min[w] = &s.value
				} else if s.value < *sv.min[w] {
					sv.min[w] = &s.value
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

func (as *minAggrState) flushState(ctx *flushCtx, flushTimestamp int64, w int) {
	m := &as.m
	m.Range(func(k, v interface{}) bool {
		sv := v.(*minStateValue)
		sv.mu.Lock()
		deleted := flushTimestamp > sv.deleteDeadline
		state := sv.min[w]
		if deleted {
			// Mark the current entry as deleted
			sv.deleted = deleted
		} else if state == nil {
			sv.mu.Unlock()
			return true
		}
		sv.min[w] = nil
		sv.mu.Unlock()
		key := k.(string)
		ctx.appendSeries(key, "min", flushTimestamp, *state)
		if deleted {
			m.Delete(k)
		}
		return true
	})
}
