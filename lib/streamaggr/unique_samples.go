package streamaggr

import (
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
)

// uniqueSamplesAggrState calculates output=unique_samples, e.g. the number of unique sample values.
type uniqueSamplesAggrState struct {
	m sync.Map
	// Time series state is dropped if no new samples are received during stalenessMsecs.
	//
	// Aslo, the first sample per each new series is ignored during stalenessMsecs even if keepFirstSample is set.
	stalenessMsecs int64
}

type uniqueSamplesStateValue struct {
	mu             sync.Mutex
	m              []map[float64]struct{}
	deleted        bool
	deleteDeadline int64
}

func newUniqueSamplesAggrState(stalenessInterval time.Duration) *uniqueSamplesAggrState {
	stalenessMsecs := durationToMsecs(stalenessInterval)
	return &uniqueSamplesAggrState{
		stalenessMsecs: stalenessMsecs,
	}
}

func (as *uniqueSamplesAggrState) pushSamples(windows [][]pushSample) {
	currentTime := fasttime.UnixMilli()
	deleteDeadline := currentTime + as.stalenessMsecs
	for ts, samples := range windows {
		for i := range samples {
			s := &samples[i]
			outputKey := getOutputKey(s.key)

		again:
			v, ok := as.m.Load(outputKey)
			if !ok {
				// The entry is missing in the map. Try creating it.
				v = &uniqueSamplesStateValue{
					m: make([]map[float64]struct{}, len(samples)),
				}
				if vNew, loaded := as.m.LoadOrStore(outputKey, v); loaded {
					v = vNew
				}
			}
			sv := v.(*uniqueSamplesStateValue)
			sv.mu.Lock()
			deleted := sv.deleted
			if !deleted {
				if _, ok = sv.m[ts][s.value]; !ok {
					sv.m[ts][s.value] = struct{}{}
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

func (as *uniqueSamplesAggrState) flushState(ctx *flushCtx, flushTimestamp int64, w int) {
	m := &as.m
	m.Range(func(k, v interface{}) bool {
		sv := v.(*uniqueSamplesStateValue)
		sv.mu.Lock()
		state := sv.m[w]
		deleted := flushTimestamp > sv.deleteDeadline
		if deleted {
			// Mark the current entry as deleted
			sv.deleted = deleted
		}
		sv.mu.Unlock()
		key := k.(string)
		ctx.appendSeries(key, "unique_samples", flushTimestamp, float64(len(state)))
		if deleted {
			m.Delete(k)
		} else {
			sv.mu.Lock()
			clear(sv.m)
			sv.mu.Unlock()
		}
		return true
	})
}
