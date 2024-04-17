package streamaggr

import (
	"math"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
)

// totalAggrState calculates output=total, e.g. the summary counter over input counters.
type totalAggrState struct {
	m sync.Map

	suffix string

	// Whether to reset the output value on every flushState call.
	resetTotalOnFlush bool

	// Whether to take into account the first sample in new time series when calculating the output value.
	keepFirstSample bool

	// Time series state is dropped if no new samples are received during stalenessMsecs.
	//
	// Aslo, the first sample per each new series is ignored during stalenessMsecs even if keepFirstSample is set.
	stalenessMsecs int64
}

type totalStateValue struct {
	mu             sync.Mutex
	lastValues     map[string]lastValueState
	total          []*float64
	totalState     float64
	deleteDeadline int64
	deleted        bool
}

type lastValueState struct {
	value          float64
	timestamp      int64
	deleteDeadline int64
}

func newTotalAggrState(stalenessInterval time.Duration, resetTotalOnFlush, keepFirstSample bool) *totalAggrState {
	stalenessMsecs := durationToMsecs(stalenessInterval)
	suffix := "total"
	if resetTotalOnFlush {
		suffix = "increase"
	}
	return &totalAggrState{
		suffix:            suffix,
		resetTotalOnFlush: resetTotalOnFlush,
		keepFirstSample:   keepFirstSample,
		stalenessMsecs:    stalenessMsecs,
	}
}

func (as *totalAggrState) pushSamples(windows [][]pushSample) {
	currentTime := fasttime.UnixMilli()
	deleteDeadline := currentTime + as.stalenessMsecs
	for w, samples := range windows {
		for i := range samples {
			s := &samples[i]
			inputKey, outputKey := getInputOutputKey(s.key)

		again:
			v, ok := as.m.Load(outputKey)
			if !ok {
				// The entry is missing in the map. Try creating it.
				v = &totalStateValue{
					lastValues: make(map[string]lastValueState),
					total:      make([]*float64, len(windows)),
				}
				if vNew, loaded := as.m.LoadOrStore(outputKey, v); loaded {
					v = vNew
				}
			}
			sv := v.(*totalStateValue)
			sv.mu.Lock()
			deleted := sv.deleted
			if !deleted {
				lv, ok := sv.lastValues[inputKey]
				if ok || as.keepFirstSample {
					if s.timestamp < lv.timestamp {
						// Skip out of order sample
						sv.mu.Unlock()
						continue
					}
					if sv.total[w] == nil {
						var t float64
						sv.total[w] = &t
					}
					if s.value >= lv.value {
						*sv.total[w] += s.value - lv.value
					} else {
						// counter reset
						*sv.total[w] += s.value
					}
				}
				lv.value = s.value
				lv.timestamp = s.timestamp
				lv.deleteDeadline = deleteDeadline
				sv.lastValues[inputKey] = lv
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

func (as *totalAggrState) removeOldEntries(currentTime int64) {
	m := &as.m
	m.Range(func(k, v interface{}) bool {
		sv := v.(*totalStateValue)
		sv.mu.Lock()
		deleted := currentTime > sv.deleteDeadline
		if deleted {
			// Mark the current entry as deleted
			sv.deleted = deleted
		} else {
			// Delete outdated entries in sv.lastValues
			m := sv.lastValues
			for k1, v1 := range m {
				if currentTime > v1.deleteDeadline {
					delete(m, k1)
				}
			}
		}
		sv.mu.Unlock()

		if deleted {
			m.Delete(k)
		}
		return true
	})
}

func (as *totalAggrState) flushState(ctx *flushCtx, flushTimestamp int64, w int) {
	as.removeOldEntries(flushTimestamp)
	m := &as.m
	m.Range(func(k, v interface{}) bool {
		sv := v.(*totalStateValue)
		sv.mu.Lock()
		state := sv.total[w]
		if state == nil {
			sv.mu.Unlock()
			return true
		}
		sv.total[w] = nil
		deleted := sv.deleted
		if as.resetTotalOnFlush {
			sv.totalState = 0
		} else if math.Abs(*state) >= (1 << 53) {
			// It is time to reset the entry, since it starts losing float64 precision
			sv.totalState = 0
		} else {
			*state += sv.totalState.Load()
			sv.totalState = *state
		}
		sv.mu.Unlock()
		if !deleted {
			key := k.(string)
			ctx.appendSeries(key, as.suffix, flushTimestamp, *state)
		}
		return true
	})
}
