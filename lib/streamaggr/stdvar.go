package streamaggr

import (
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
)

// stdvarAggrState calculates output=stdvar, e.g. the average value over input samples.
type stdvarAggrState struct {
	m sync.Map
	// Time series state is dropped if no new samples are received during stalenessMsecs.
	//
	// Aslo, the first sample per each new series is ignored during stalenessMsecs even if keepFirstSample is set.
	stalenessMsecs int64
}

type stdvarStateValue struct {
	mu             sync.Mutex
	stdvar         []*stdvarState
	deleted        bool
	deleteDeadline int64
}

type stdvarState struct {
	count float64
	avg   float64
	q     float64
}

func newStdvarAggrState(stalenessInterval time.Duration) *stdvarAggrState {
	stalenessMsecs := durationToMsecs(stalenessInterval)
	return &stdvarAggrState{
		stalenessMsecs: stalenessMsecs,
	}
}

func (as *stdvarAggrState) pushSamples(windows [][]pushSample) {
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
				v = &stdvarStateValue{
					stdvar: make([]*stdvarState, len(windows)),
				}
				if vNew, loaded := as.m.LoadOrStore(outputKey, v); loaded {
					v = vNew
				}
			}
			sv := v.(*stdvarStateValue)
			sv.mu.Lock()
			deleted := sv.deleted
			if !deleted {
				// See `Rapid calculation methods` at https://en.wikipedia.org/wiki/Standard_deviation
				if sv.stdvar[w] == nil {
					sv.stdvar[w] = &stdvarState{}
				}
				v := sv.stdvar[w]
				v.count++
				avg := v.avg + (s.value-v.avg)/v.count
				v.q += (s.value - v.avg) * (s.value - avg)
				v.avg = avg
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

func (as *stdvarAggrState) flushState(ctx *flushCtx, flushTimestamp int64, w int) {
	m := &as.m
	m.Range(func(k, v interface{}) bool {
		sv := v.(*stdvarStateValue)
		sv.mu.Lock()
		deleted := flushTimestamp > sv.deleteDeadline
		state := sv.stdvar[w]
		if deleted {
			// Mark the current entry as deleted
			sv.deleted = deleted
		} else if state == nil {
			sv.mu.Unlock()
			return true
		}
		sv.stdvar[w] = nil
		sv.mu.Unlock()
		stdvar := state.q / state.count
		key := k.(string)
		ctx.appendSeries(key, "stdvar", flushTimestamp, stdvar)
		if deleted {
			m.Delete(k)
		}
		return true
	})
}
