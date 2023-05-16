package utils

import (
	"database/sql"
	"encoding/json"
	"time"
)

type SQLNullTime struct {
	sql.NullTime
}

func (s *SQLNullTime) MarshalJSON() ([]byte, error) {
	if s.Valid {
		return json.Marshal(s.Time)
	}
	return nil, nil
}

func (s *SQLNullTime) UnmarshalJSON(data []byte) (err error) {
	t := time.Time{}
	err = json.Unmarshal(data, &t)
	if err != nil {
		return err
	}
	s.SetTime(t)
	return nil
}

func NewTime(t time.Time) *SQLNullTime {
	if !t.IsZero() {
		return &SQLNullTime{
			NullTime: sql.NullTime{
				Time:  t,
				Valid: true,
			},
		}
	}
	return nil
}

func (s *SQLNullTime) SetTime(t time.Time) {
	s.Time = t
	if t.IsZero() {
		s.Valid = false
		return
	}
	s.Valid = true
}

type Timer struct {
	StartTime *SQLNullTime  `json:"start_time,omitempty"`
	Duration  time.Duration `json:"duration,omitempty"`
	StopTime  *SQLNullTime  `json:"stop_time,omitempty"`
}

func NewTimer() *Timer {
	return &Timer{
		StartTime: NewTime(time.Now()),
		StopTime:  nil,
	}
}

func (t *Timer) Elapsed() time.Duration {
	return time.Since(t.StartTime.Time)
}

func (t *Timer) ElapsedSeconds() float64 {
	return float64(t.Elapsed()) / float64(time.Second)
}

func (t *Timer) Stop() {
	t.StopTime = NewTime(time.Now())
	t.Duration = t.StopTime.Time.Sub(t.StartTime.Time)
}
