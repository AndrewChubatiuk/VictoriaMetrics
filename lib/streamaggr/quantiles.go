package streamaggr

import (
	"strconv"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fasttime"
	"github.com/valyala/histogram"
)

// quantilesAggrState calculates output=quantiles, e.g. the the given quantiles over the input samples.
type quantilesAggrState struct {
	m    sync.Map
	phis []float64

	// Time series state is dropped if no new samples are received during stalenessMsecs.
	//
	// Aslo, the first sample per each new series is ignored during stalenessMsecs even if keepFirstSample is set.
	stalenessMsecs int64
}

type quantilesStateValue struct {
	mu             sync.Mutex
	h              []*histogram.Fast
	deleted        bool
	deleteDeadline int64
}

func newQuantilesAggrState(stalenessInterval time.Duration, phis []float64) *quantilesAggrState {
	stalenessMsecs := durationToMsecs(stalenessInterval)
	return &quantilesAggrState{
		phis:           phis,
		stalenessMsecs: stalenessMsecs,
	}
}

func (as *quantilesAggrState) pushSamples(windows [][]pushSample) {
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
				v = &quantilesStateValue{
					h: make([]*histogram.Fast, len(windows)),
				}
				if vNew, loaded := as.m.LoadOrStore(outputKey, v); loaded {
					v = vNew
				}
			}
			sv := v.(*quantilesStateValue)
			sv.mu.Lock()
			deleted := sv.deleted
			if !deleted {
				if sv.h[w] == nil {
					sv.h[w] = histogram.GetFast()
				}
				sv.h[w].Update(s.value)
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

func (as *quantilesAggrState) flushState(ctx *flushCtx, flushTimestamp int64, w int) {
	m := &as.m
	phis := as.phis
	var quantiles []float64
	var b []byte
	m.Range(func(k, v interface{}) bool {
		sv := v.(*quantilesStateValue)
		sv.mu.Lock()
		deleted := flushTimestamp > sv.deleteDeadline
		state := sv.h[w]
		if deleted {
			// Mark the current entry as deleted
			sv.deleted = deleted
		} else if state == nil {
			sv.mu.Unlock()
			return true
		}
		sv.h[w] = nil
		sv.mu.Unlock()
		quantiles = state.Quantiles(quantiles[:0], phis)
		histogram.PutFast(state)
		key := k.(string)
		for i, quantile := range quantiles {
			b = strconv.AppendFloat(b[:0], phis[i], 'g', -1, 64)
			phiStr := bytesutil.InternBytes(b)
			ctx.appendSeriesWithExtraLabel(key, "quantiles", flushTimestamp, quantile, "quantile", phiStr)
		}
		if deleted {
			m.Delete(k)
		}
		return true
	})
}
