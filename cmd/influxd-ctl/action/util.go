package action

import "time"

const (
	TIME_FORMAT = "2006-01-02 15:04:05"
)

func formatTimeStamp(millis int64) string {
	t := time.Unix(millis/1000, (millis%1000)*1e6)
	return t.Local().Format(TIME_FORMAT)
}
