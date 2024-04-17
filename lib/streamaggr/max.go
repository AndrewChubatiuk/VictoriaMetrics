package streamaggr

import (
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
)

// maxAggrState calculates output=max, e.g. the maximum value over input samples.
type maxAggrState struct {
	m sync.Map

	// Time series state is dropped if no new samples are received during stalenessMsecs.
	//
	// Aslo, the first sample per each new series is ignored during stalenessMsecs even if keepFirstSample is set.
	stalenessMsecs int64
}

type maxStateValue struct {
	mu             sync.Mutex
	max            []*float64
	deleted        bool
	deleteDeadline int64
}

func newMaxAggrState(stalenessInterval time.Duration) *maxAggrState {
	stalenessMsecs := durationToMsecs(stalenessInterval)
	return &maxAggrState{
		stalenessMsecs: stalenessMsecs,
	}
}

func (as *maxAggrState) pushSamples(windows [][]pushSample) {
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
				v = &maxStateValue{
					max: make([]*float64, len(windows)),
				}
				if vNew, loaded := as.m.LoadOrStore(outputKey, v); loaded {
					v = vNew
				}
			}
			sv := v.(*maxStateValue)
			sv.mu.Lock()
			deleted := sv.deleted
			if !deleted {
				if sv.max[w] == nil {
					sv.max[w] = &s.value
				} else if s.value > *sv.max[w] {
					sv.max[w] = &s.value
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

func (as *maxAggrState) flushState(ctx *flushCtx, flushTimestamp int64, w int) {
	m := &as.m
	m.Range(func(k, v interface{}) bool {
		sv := v.(*maxStateValue)
		sv.mu.Lock()
		deleted := flushTimestamp > sv.deleteDeadline
		state := sv.max[w]
		if deleted {
			// Mark the current entry as deleted
			sv.deleted = deleted
		} else if state == nil {
			sv.mu.Unlock()
			return true
		}
		sv.max[w] = nil
		sv.mu.Unlock()
		key := k.(string)
		ctx.appendSeries(key, "max", flushTimestamp, *state)
		if deleted {
			m.Delete(k)
		}
		return true
	})
}
