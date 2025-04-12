package syrotest

import "time"

type TimeseriesFilter struct {
	From  time.Time
	To    time.Time
	Limit int64
	Skip  int64
}
