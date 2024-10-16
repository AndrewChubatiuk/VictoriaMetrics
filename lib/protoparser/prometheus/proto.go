package prometheus

import (
	"fmt"
	"time"

	"github.com/VictoriaMetrics/easyproto"
)

// Timestamp struct stores information about metric time
//
//	message Timestamp {
//	  int64 seconds = 1;
//	  int32 nanos = 2;
//	}
//
// See https://github.com/protocolbuffers/protobuf/blob/b785fcb5834cd94954b4f81a0310af0d5459f519/src/google/protobuf/timestamp.proto#L133
type Timestamp struct {
	Milli int64
}

// unmarshalProtobuf decodes src to Timestamp struct
func (t *Timestamp) unmarshalProtobuf(src []byte) (err error) {
	var seconds int64
	var nanos int32
	var ok bool
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in Timestamp message: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			seconds, ok = fc.Int64()
			if !ok {
				return fmt.Errorf("cannot read seconds")
			}
		case 2:
			nanos, ok = fc.Int32()
			if !ok {
				return fmt.Errorf("cannot read nanos")
			}
		}
	}
	t.Milli = time.Unix(seconds, int64(nanos)).UTC().UnixMilli()
	return nil
}

// MetricType enum stores all supported metric types
//
//	message MetricType {
//		COUNTER         = 0;
//		GAUGE           = 1;
//		SUMMARY         = 2;
//		UNTYPED         = 3;
//		HISTOGRAM       = 4;
//		GAUGE_HISTOGRAM = 5;
//	}
//
// See https://github.com/prometheus/prometheus/blob/aba007148057c1947122b18b2ad606883cc27220/prompb/io/prometheus/client/metrics.proto#L32
type MetricType uint8

const (
	// CounterType is for counter metric type
	CounterType MetricType = 0
	// GaugeType is for gauge metric type
	GaugeType MetricType = 1
	// SummaryType is for summary metric type
	SummaryType MetricType = 2
	// UntypedType is for untyped metric type
	UntypedType MetricType = 3
	// HistogramType is for histogram metric type
	HistogramType MetricType = 4
	// GaugeHistogramType metric type
	GaugeHistogramType MetricType = 5
)

var mp easyproto.MarshalerPool

// ProtoRequest stores collection of metric families
type ProtoRequest struct {
	Families []*MetricFamily
}

func protobufRange(src []byte, process func([]byte) error) error {
	var ok bool
	var messageLen int
	for len(src) > 0 {
		messageLen, src, ok = easyproto.UnmarshalMessageLen(src)
		if !ok {
			return fmt.Errorf("failed to read message length")
		}
		if messageLen > len(src) {
			return fmt.Errorf("invalid message data")
		}
		if err := process(src[:messageLen]); err != nil {
			return fmt.Errorf("failed to process message item: %s", err)
		}
		src = src[messageLen:]
	}
	return nil
}

func (r *ProtoRequest) unmarshalProtobuf(src []byte) error {
	return protobufRange(src, func(dst []byte) error {
		f := &MetricFamily{}
		if err := f.unmarshalProtobuf(dst, false); err != nil {
			return fmt.Errorf("cannot parse MetricFamily: %w", err)
		}
		r.Families = append(r.Families, f)
		return nil
	})
}

func (r *ProtoRequest) marshalProtobuf(dst []byte) []byte {
	m := mp.Get()
	for _, f := range r.Families {
		mm := m.MessageMarshaler()
		f.marshalProtobuf(mm)
		dst = m.MarshalWithLen(dst)
		m.Reset()
	}
	mp.Put(m)
	return dst
}

// MetricFamily stores collection of Prometheus metrics
//
//	message MetricFamily {
//	  string          name   = 1;
//	  string          help   = 2;
//	  MetricType      type   = 3;
//	  repeated Metric metric = 4;
//	  string          unit   = 5;
//	}
//
// See https://github.com/prometheus/prometheus/blob/aba007148057c1947122b18b2ad606883cc27220/prompb/io/prometheus/client/metrics.proto#L154
type MetricFamily struct {
	Name    string
	Type    MetricType
	Metrics []*Metric
}

func (f *MetricFamily) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	mm.AppendString(1, f.Name)
	mm.AppendInt32(3, int32(f.Type))
	for _, m := range f.Metrics {
		m.marshalProtobuf(mm.AppendMessage(4))
	}
}

func (f *MetricFamily) getRows() []Row {
	rows := make([]Row, len(f.Metrics))
	for i, m := range f.Metrics {
		rows[i].Metric = fmt.Sprintf("%s_%d", f.Name, f.Type)
		rows[i].Tags = m.Tags
	}
	return rows
}

// unmarshalProtobuf decodes src to MetricFamily struct
func (f *MetricFamily) unmarshalProtobuf(src []byte, partial bool) (err error) {
	f.Metrics = nil
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in MetricFamily message: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			name, ok := fc.String()
			if !ok {
				return fmt.Errorf("cannot read name")
			}
			f.Name = name
		case 3:
			metricType, ok := fc.Int32()
			if !ok {
				return fmt.Errorf("cannot read type")
			}
			f.Type = MetricType(metricType)
		case 4:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read Metric data")
			}
			m := &Metric{}
			if err := m.unmarshalProtobuf(data, partial); err != nil {
				return fmt.Errorf("cannot unmarshal Metric: %w", err)
			}
			f.Metrics = append(f.Metrics, m)
		}
	}
	return nil
}

// Metric stores Prometheus metric
//
//	message Metric {
//	  repeated LabelPair label        = 1;
//	  Gauge              gauge        = 2;
//	  Counter            counter      = 3;
//	  Summary            summary      = 4;
//	  Untyped            untyped      = 5;
//	  Histogram          histogram    = 7;
//	  int64              timestamp_ms = 6;
//	}
//
// See https://github.com/prometheus/prometheus/blob/aba007148057c1947122b18b2ad606883cc27220/prompb/io/prometheus/client/metrics.proto#L144
type Metric struct {
	Tags      []Tag
	Gauge     *Gauge
	Counter   *Counter
	Summary   *Summary
	Untyped   *Untyped
	Histogram *Histogram
	Timestamp int64
	raw       []byte
}

func (m *Metric) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	for _, t := range m.Tags {
		t.marshalProtobuf(mm.AppendMessage(1))
	}
	if len(m.raw) > 0 {
		mm.AppendRaw(m.raw)
	}
}

// unmarshalProtobuf decodes src to Metric struct
func (m *Metric) unmarshalProtobuf(src []byte, partial bool) error {
	m.Tags = nil
	var fc easyproto.FieldContext
	var tmp []byte
	var err error
	target := src
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in Metric message: %w", err)
		}
		if partial {
			tmp = target[:len(target)-len(src)]
			target = src
		}

		switch fc.FieldNum {
		case 1:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read Tag data")
			}
			var t Tag
			if err := (&t).unmarshalProtobuf(data); err != nil {
				return fmt.Errorf("cannot unmarshal Tag: %w", err)
			}
			m.Tags = append(m.Tags, t)
		case 2:
			if partial {
				m.raw = append(m.raw, tmp...)
				continue
			}
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read gauge data")
			}
			var gauge Gauge
			if err := gauge.unmarshalProtobuf(data); err != nil {
				return fmt.Errorf("cannot unmarshal gauge: %w", err)
			}
			m.Gauge = &gauge
		case 3:
			if partial {
				m.raw = append(m.raw, tmp...)
				continue
			}
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read counter data")
			}
			var counter Counter
			if err := counter.unmarshalProtobuf(data); err != nil {
				return fmt.Errorf("cannot unmarshal counter: %w", err)
			}
			m.Counter = &counter
		case 4:
			if partial {
				m.raw = append(m.raw, tmp...)
				continue
			}
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read summary data")
			}
			var summary Summary
			if err := summary.unmarshalProtobuf(data); err != nil {
				return fmt.Errorf("cannot unmarshal summary: %w", err)
			}
			m.Summary = &summary
		case 5:
			if partial {
				m.raw = append(m.raw, tmp...)
				continue
			}
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read untyped data")
			}
			var untyped Untyped
			if err := untyped.unmarshalProtobuf(data); err != nil {
				return fmt.Errorf("cannot unmarshal Untyped: %w", err)
			}
			m.Untyped = &untyped
		case 7:
			if partial {
				m.raw = append(m.raw, tmp...)
				continue
			}
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read histogram data")
			}
			var histogram Histogram
			if err := histogram.unmarshalProtobuf(data); err != nil {
				return fmt.Errorf("cannot unmarshal Histogram: %w", err)
			}
			m.Histogram = &histogram
		case 6:
			if partial {
				m.raw = append(m.raw, tmp...)
				continue
			}
			ts, ok := fc.Int64()
			if !ok {
				return fmt.Errorf("cannot read timestamp")
			}
			m.Timestamp = ts
		}
	}
	return nil
}

// unmarshalProtobuf decodes src to Tag struct
func (t *Tag) unmarshalProtobuf(src []byte) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in Tag message: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			key, ok := fc.String()
			if !ok {
				return fmt.Errorf("cannot read name")
			}
			t.Key = key
		case 2:
			value, ok := fc.String()
			if !ok {
				return fmt.Errorf("cannot read value")
			}
			t.Value = value
		}
	}
	return nil
}

func (t *Tag) marshalProtobuf(mm *easyproto.MessageMarshaler) {
	mm.AppendString(1, t.Key)
	mm.AppendString(2, t.Value)
}

// Gauge stores Prometheus metric gauge value
//
//	message Gauge {
//	  double value = 1;
//	}
//
// See https://github.com/prometheus/prometheus/blob/aba007148057c1947122b18b2ad606883cc27220/prompb/io/prometheus/client/metrics.proto#L32
type Gauge struct {
	Value float64
}

// unmarshalProtobuf decodes src to Gauge struct
func (g *Gauge) unmarshalProtobuf(src []byte) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in Gauge message: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			value, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read value")
			}
			g.Value = value
		}
	}
	return nil
}

// Counter stores Prometheus metric counter value
//
//	message Counter {
//	  double   value    = 1;
//	  Exemplar exemplar = 2;
//	}
//
// See https://github.com/prometheus/prometheus/blob/aba007148057c1947122b18b2ad606883cc27220/prompb/io/prometheus/client/metrics.proto#L47
type Counter struct {
	Value float64
}

// unmarshalProtobuf decodes src to Counter struct
func (c *Counter) unmarshalProtobuf(src []byte) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in Counter message: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			value, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read value")
			}
			c.Value = value
		}
	}
	return nil
}

// Quantile stores Prometheus metric quantile
//
//	message Quantile {
//	  double   quantile = 1;
//	  double   value    = 2;
//	}
//
// See https://github.com/prometheus/prometheus/blob/aba007148057c1947122b18b2ad606883cc27220/prompb/io/prometheus/client/metrics.proto#L58
type Quantile struct {
	Value    float64
	Quantile float64
}

// unmarshalProtobuf decodes src to Quantile struct
func (q *Quantile) unmarshalProtobuf(src []byte) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in Quantile message: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			quantile, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read quantile")
			}
			q.Quantile = quantile
		case 2:
			value, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read value")
			}
			q.Value = value
		}
	}
	return nil
}

// Summary stores Prometheus metric summary
//
//	message Summary {
//	  uint64            sample_count = 1;
//	  double            sample_sum   = 2;
//	  repeated Quantile quantile     = 3;
//	}
//
// See https://github.com/prometheus/prometheus/blob/aba007148057c1947122b18b2ad606883cc27220/prompb/io/prometheus/client/metrics.proto#L63
type Summary struct {
	SampleCount uint64
	SampleSum   float64
	Quantiles   []*Quantile
}

// unmarshalProtobuf decodes src to Summary struct
func (s *Summary) unmarshalProtobuf(src []byte) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in Summary message: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			sampleCount, ok := fc.Uint64()
			if !ok {
				return fmt.Errorf("cannot read sample_count")
			}
			s.SampleCount = sampleCount
		case 2:
			sampleSum, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read sample_sum")
			}
			s.SampleSum = sampleSum
		case 3:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read Quantiles data")
			}
			var q Quantile
			if err := q.unmarshalProtobuf(data); err != nil {
				return fmt.Errorf("cannot unmarshal Tag: %w", err)
			}
			s.Quantiles = append(s.Quantiles, &q)
		}
	}
	return nil
}

// Untyped stores Prometheus metric untyped value
//
//	message Untyped {
//	  double value = 1;
//	}
//
// See https://github.com/prometheus/prometheus/blob/aba007148057c1947122b18b2ad606883cc27220/prompb/io/prometheus/client/metrics.proto#L71
type Untyped struct {
	Value float64
}

// unmarshalProtobuf decodes src to Untyped struct
func (u *Untyped) unmarshalProtobuf(src []byte) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in Gauge message: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			value, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read value")
			}
			u.Value = value
		}
	}
	return nil
}

// Histogram stores Prometheus histogram
//
//	message Histogram {
//	  uint64                    sample_count       = 1;
//	  double                    sample_count_float = 4;
//	  double                    sample_sum         = 2;
//	  repeated Bucket           bucket             = 3;
//	  google.protobuf.Timestamp created_timestamp  = 15;
//	  sint32                    schema             = 5;
//	  double                    zero_threshold     = 6;
//	  uint64                    zero_count         = 7;
//	  double                    zero_count_float   = 8;
//	  repeated BucketSpan       negative_span      = 9;
//	  repeated sint64           negative_delta     = 10;
//	  repeated double           negative_count     = 11;
//	  repeated BucketSpan       positive_span      = 12;
//	  repeated sint64           positive_delta     = 13;
//	  repeated double           positive_count     = 14;
//	  repeated Exemplar         exemplars          = 16;
//	}
//
// See https://github.com/prometheus/prometheus/blob/aba007148057c1947122b18b2ad606883cc27220/prompb/io/prometheus/client/metrics.proto#L75
type Histogram struct {
	SampleCount      uint64
	SampleCountFloat float64
	SampleSum        float64
	Buckets          []*Bucket
	CreatedTimestamp *Timestamp
	Schema           int32
	ZeroThreshold    float64
	ZeroCount        uint64
	ZeroCountFloat   float64
	NegativeSpans    []*BucketSpan
	NegativeDeltas   []int64
	NegativeCounts   []float64
	PositiveSpans    []*BucketSpan
	PositiveDeltas   []int64
	PositiveCounts   []float64
}

// unmarshalProtobuf decodes src to Histogram struct
func (h *Histogram) unmarshalProtobuf(src []byte) (err error) {
	h.Buckets = nil
	h.NegativeSpans = nil
	h.NegativeDeltas = nil
	h.NegativeCounts = nil
	h.PositiveSpans = nil
	h.PositiveDeltas = nil
	h.PositiveCounts = nil
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in Metric message: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			sampleCount, ok := fc.Uint64()
			if !ok {
				return fmt.Errorf("cannot read sample_count")
			}
			h.SampleCount = sampleCount
		case 2:
			sampleSum, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read sample_sum")
			}
			h.SampleSum = sampleSum
		case 3:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read bucket data")
			}
			var b Bucket
			if err := b.unmarshalProtobuf(data); err != nil {
				return fmt.Errorf("cannot unmarshal bucket: %w", err)
			}
			h.Buckets = append(h.Buckets, &b)
		case 4:
			sampleCountFloat, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read sample_count_float")
			}
			h.SampleCountFloat = sampleCountFloat
		case 5:
			schema, ok := fc.Sint32()
			if !ok {
				return fmt.Errorf("cannot read schema")
			}
			h.Schema = schema
		case 6:
			zeroThreshold, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read zero_threshold")
			}
			h.ZeroThreshold = zeroThreshold
		case 7:
			zeroCount, ok := fc.Uint64()
			if !ok {
				return fmt.Errorf("cannot read zero_count")
			}
			h.ZeroCount = zeroCount
		case 8:
			zeroCountFloat, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read zero_count_float")
			}
			h.ZeroCountFloat = zeroCountFloat
		case 9:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read negative_span data")
			}
			var s BucketSpan
			if err := s.unmarshalProtobuf(data); err != nil {
				return fmt.Errorf("cannot unmarshal negative_span: %w", err)
			}
			h.NegativeSpans = append(h.NegativeSpans, &s)
		case 10:
			var ok bool
			h.NegativeDeltas, ok = fc.UnpackSint64s(h.NegativeDeltas)
			if !ok {
				return fmt.Errorf("cannot read negative_delta")
			}
		case 11:
			var ok bool
			h.NegativeCounts, ok = fc.UnpackDoubles(h.NegativeCounts)
			if !ok {
				return fmt.Errorf("cannot read negative_count")
			}
		case 12:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read positive_span data")
			}
			var s BucketSpan
			if err := s.unmarshalProtobuf(data); err != nil {
				return fmt.Errorf("cannot unmarshal positive_span: %w", err)
			}
			h.PositiveSpans = append(h.PositiveSpans, &s)
		case 13:
			var ok bool
			h.PositiveDeltas, ok = fc.UnpackSint64s(h.PositiveDeltas)
			if !ok {
				return fmt.Errorf("cannot read positive_delta")
			}
		case 14:
			var ok bool
			h.PositiveCounts, ok = fc.UnpackDoubles(h.PositiveCounts)
			if !ok {
				return fmt.Errorf("cannot read positive_count")
			}
		case 15:
			data, ok := fc.MessageData()
			if !ok {
				return fmt.Errorf("cannot read created_timestamp data")
			}
			var timestamp Timestamp
			if err := timestamp.unmarshalProtobuf(data); err != nil {
				return fmt.Errorf("cannot unmarshal created_timestamp: %w", err)
			}
			h.CreatedTimestamp = &timestamp
		}
	}
	return nil
}

// Bucket stores Prometheus metric's bucket
//
//	message Bucket {
//	  uint64   cumulative_count       = 1;
//	  double   cumulative_count_float = 4;
//	  double   upper_bound            = 2;
//	  Exemplar exemplar               = 3;
//	}
//
// See https://github.com/prometheus/prometheus/blob/aba007148057c1947122b18b2ad606883cc27220/prompb/io/prometheus/client/metrics.proto#L120
type Bucket struct {
	CumulativeCount      uint64
	CumulativeCountFloat float64
	UpperBound           float64
}

// unmarshalProtobuf decodes src to Bucket struct
func (b *Bucket) unmarshalProtobuf(src []byte) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in Bucket message: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			cumulativeCount, ok := fc.Uint64()
			if !ok {
				return fmt.Errorf("cannot read cumulative_count")
			}
			b.CumulativeCount = cumulativeCount
		case 2:
			upperBound, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read upper_bound")
			}
			b.UpperBound = upperBound
		case 4:
			cumulativeCountFloat, ok := fc.Double()
			if !ok {
				return fmt.Errorf("cannot read cumulative_count_float")
			}
			b.CumulativeCountFloat = cumulativeCountFloat
		}
	}
	return nil
}

// BucketSpan stores Prometheus metric's bucket's span
//
//	message BucketSpan {
//	  sint64 offset = 1;
//	  uint32 length = 2;
//	}
//
// See https://github.com/prometheus/prometheus/blob/aba007148057c1947122b18b2ad606883cc27220/prompb/io/prometheus/client/metrics.proto#L133
type BucketSpan struct {
	Offset int32
	Length uint32
}

// unmarshalProtobuf decodes src to BucketSpan struct
func (b *BucketSpan) unmarshalProtobuf(src []byte) (err error) {
	var fc easyproto.FieldContext
	for len(src) > 0 {
		src, err = fc.NextField(src)
		if err != nil {
			return fmt.Errorf("cannot read next field in BucketSpan message: %w", err)
		}
		switch fc.FieldNum {
		case 1:
			offset, ok := fc.Sint32()
			if !ok {
				return fmt.Errorf("cannot read offset")
			}
			b.Offset = offset
		case 2:
			l, ok := fc.Uint32()
			if !ok {
				return fmt.Errorf("cannot read length")
			}
			b.Length = l
		}
	}
	return nil
}