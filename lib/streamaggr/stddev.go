package streamaggr

import (
	"math"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
)

// stddevAggrState calculates output=stddev, e.g. the average value over input samples.
type stddevAggrState struct {
	m sync.Map
	// Time series state is dropped if no new samples are received during stalenessMsecs.
	//
	// Aslo, the first sample per each new series is ignored during stalenessMsecs even if keepFirstSample is set.
	stalenessMsecs int64
}

type stddevStateValue struct {
	mu             sync.Mutex
	stddev         []*stddevState
	deleted        bool
	deleteDeadline int64
}

type stddevState struct {
	count float64
	avg   float64
	q     float64
}

func newStddevAggrState(stalenessInterval time.Duration) *stddevAggrState {
	stalenessMsecs := durationToMsecs(stalenessInterval)
	return &stddevAggrState{
		stalenessMsecs: stalenessMsecs,
	}
}

func (as *stddevAggrState) pushSamples(windows [][]pushSample) {
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
				v = &stddevStateValue{
					stddev: make([]*stddevState, len(windows)),
				}
				if vNew, loaded := as.m.LoadOrStore(outputKey, v); loaded {
					v = vNew
				}
			}
			sv := v.(*stddevStateValue)
			sv.mu.Lock()
			deleted := sv.deleted
			if !deleted {
				// See `Rapid calculation methods` at https://en.wikipedia.org/wiki/Standard_deviation
				if sv.stddev[w] == nil {
					sv.stddev[w] = &stddevState{}
				}
				v := sv.stddev[w]
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

func (as *stddevAggrState) flushState(ctx *flushCtx, flushTimestamp int64, w int) {
	m := &as.m
	m.Range(func(k, v interface{}) bool {
		sv := v.(*stddevStateValue)
		sv.mu.Lock()
		deleted := flushTimestamp > sv.deleteDeadline
		state := sv.stddev[w]
		if deleted {
			// Mark the current entry as deleted
			sv.deleted = deleted
		} else if state == nil {
			sv.mu.Unlock()
			return true
		}
		sv.stddev[w] = nil
		sv.mu.Unlock()
		stddev := math.Sqrt(state.q / state.count)
		key := k.(string)
		ctx.appendSeries(key, "stddev", flushTimestamp, stddev)
		if deleted {
			m.Delete(k)
		}
		return true
	})
}
