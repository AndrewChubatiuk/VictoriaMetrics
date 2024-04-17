package streamaggr

import (
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
)

// avgAggrState calculates output=avg, e.g. the average value over input samples.
type avgAggrState struct {
	m sync.Map

	// Time series state is dropped if no new samples are received during stalenessMsecs.
	//
	// Aslo, the first sample per each new series is ignored during stalenessMsecs even if keepFirstSample is set.
	stalenessMsecs int64
}

type avgStateValue struct {
	mu             sync.Mutex
	avg            []*avgState
	deleted        bool
	deleteDeadline int64
}

type avgState struct {
	sum   float64
	count int64
}

func newAvgAggrState(stalenessInterval time.Duration) *avgAggrState {
	stalenessMsecs := durationToMsecs(stalenessInterval)
	return &avgAggrState{
		stalenessMsecs: stalenessMsecs,
	}
}

func (as *avgAggrState) pushSamples(windows [][]pushSample) {
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
				v = &avgStateValue{
					avg: make([]*avgState, len(windows)),
				}
				if vNew, loaded := as.m.LoadOrStore(outputKey, v); loaded {
					v = vNew
				}
			}
			sv := v.(*avgStateValue)
			sv.mu.Lock()
			deleted := sv.deleted
			if !deleted {
				if sv.avg[w] == nil {
					sv.avg[w] = &avgState{}
				}
				avg := sv.avg[w]
				avg.sum += s.value
				avg.count++
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

func (as *avgAggrState) flushState(ctx *flushCtx, flushTimestamp int64, w int) {
	m := &as.m
	m.Range(func(k, v interface{}) bool {
		sv := v.(*avgStateValue)
		sv.mu.Lock()
		deleted := flushTimestamp > sv.deleteDeadline
		state := sv.avg[w]
		if deleted {
			// Mark the current entry as deleted
			sv.deleted = deleted
		} else if state == nil {
			sv.mu.Unlock()
			return true
		}
		sv.avg[w] = nil
		sv.mu.Unlock()
		avg := state.sum / float64(state.count)
		key := k.(string)
		ctx.appendSeries(key, "avg", flushTimestamp, avg)
		if deleted {
			m.Delete(k)
		}
		return true
	})
}
