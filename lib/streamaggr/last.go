package streamaggr

import (
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
)

// lastAggrState calculates output=last, e.g. the last value over input samples.
type lastAggrState struct {
	m sync.Map

	// Time series state is dropped if no new samples are received during stalenessMsecs.
	//
	// Aslo, the first sample per each new series is ignored during stalenessMsecs even if keepFirstSample is set.
	stalenessMsecs int64
}

type lastStateValue struct {
	mu             sync.Mutex
	last           []*lastState
	deleted        bool
	deleteDeadline int64
}

type lastState struct {
	value     float64
	timestamp int64
}

func newLastAggrState(stalenessInterval time.Duration) *lastAggrState {
	stalenessMsecs := durationToMsecs(stalenessInterval)
	return &lastAggrState{
		stalenessMsecs: stalenessMsecs,
	}
}

func (as *lastAggrState) pushSamples(windows [][]pushSample) {
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
				v = &lastStateValue{
					last: make([]*lastState, len(windows)),
				}
				if vNew, loaded := as.m.LoadOrStore(outputKey, v); loaded {
					v = vNew
				}
			}
			sv := v.(*lastStateValue)
			sv.mu.Lock()
			deleted := sv.deleted
			if !deleted {
				if sv.last[w] == nil {
					sv.last[w] = &lastState{}
				}
				if s.timestamp >= sv.last[w].timestamp {
					sv.last[w].value = s.value
					sv.last[w].timestamp = s.timestamp
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

func (as *lastAggrState) flushState(ctx *flushCtx, flushTimestamp int64, w int) {
	m := &as.m
	m.Range(func(k, v interface{}) bool {
		sv := v.(*lastStateValue)
		sv.mu.Lock()
		deleted := flushTimestamp > sv.deleteDeadline
		state := sv.last[w]
		if deleted {
			// Mark the current entry as deleted
			sv.deleted = deleted
		} else if state == nil {
			sv.mu.Unlock()
			return true
		}
		sv.last[w] = nil
		sv.mu.Unlock()
		key := k.(string)
		ctx.appendSeries(key, "last", flushTimestamp, state.value)
		if deleted {
			m.Delete(k)
		}
		return true
	})
}
