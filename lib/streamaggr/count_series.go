package streamaggr

import (
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/cespare/xxhash/v2"
)

// countSeriesAggrState calculates output=count_series, e.g. the number of unique series.
type countSeriesAggrState struct {
	m sync.Map

	// Time series state is dropped if no new samples are received during stalenessMsecs.
	//
	// Aslo, the first sample per each new series is ignored during stalenessMsecs even if keepFirstSample is set.
	stalenessMsecs int64
}

type countSeriesStateValue struct {
	mu             sync.Mutex
	m              []map[uint64]struct{}
	deleted        bool
	deleteDeadline int64
}

func newCountSeriesAggrState(stalenessInterval time.Duration) *countSeriesAggrState {
	stalenessMsecs := durationToMsecs(stalenessInterval)
	return &countSeriesAggrState{
		stalenessMsecs: stalenessMsecs,
	}
}

func (as *countSeriesAggrState) pushSamples(windows [][]pushSample) {
	currentTime := fasttime.UnixMilli()
	deleteDeadline := currentTime + as.stalenessMsecs
	for w, samples := range windows {
		for i := range samples {
			s := &samples[i]
			inputKey, outputKey := getInputOutputKey(s.key)

			// Count unique hashes over the inputKeys instead of unique inputKey values.
			// This reduces memory usage at the cost of possible hash collisions for distinct inputKey values.
			h := xxhash.Sum64(bytesutil.ToUnsafeBytes(inputKey))

		again:
			v, ok := as.m.Load(outputKey)
			if !ok {
				// The entry is missing in the map. Try creating it.
				v = &countSeriesStateValue{
					m: make([]map[uint64]struct{}, len(windows)),
				}
				if vNew, loaded := as.m.LoadOrStore(outputKey, v); loaded {
					v = vNew
				}
			}
			sv := v.(*countSeriesStateValue)
			sv.mu.Lock()
			deleted := sv.deleted
			if !deleted {
				if sv.m[w] == nil {
					sv.m[w] = make(map[uint64]struct{})
				}
				if _, ok := sv.m[w][h]; !ok {
					sv.m[w][h] = struct{}{}
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

func (as *countSeriesAggrState) flushState(ctx *flushCtx, flushTimestamp int64, w int) {
	m := &as.m
	m.Range(func(k, v interface{}) bool {
		sv := v.(*countSeriesStateValue)
		sv.mu.Lock()
		deleted := flushTimestamp > sv.deleteDeadline
		state := sv.m[w]
		if deleted {
			// Mark the current entry as deleted
			sv.deleted = deleted
		} else if state == nil {
			sv.mu.Unlock()
			return true
		}
		sv.mu.Unlock()
		key := k.(string)
		ctx.appendSeries(key, "count_series", flushTimestamp, float64(len(state)))
		if deleted {
			m.Delete(k)
		} else {
			sv.mu.Lock()
			clear(sv.m[w])
			sv.mu.Unlock()
		}
		return true
	})
}
