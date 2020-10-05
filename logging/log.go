package logging

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/influxdata/influxdb/pkg/snowflake"
	zaplogfmt "github.com/jsternberg/zap-logfmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	gen = snowflake.New(0)
)

const TimeFormat = "2006-01-02 15:04:05"

func newEncoderConfig() zapcore.EncoderConfig {
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = func(ts time.Time, encoder zapcore.PrimitiveArrayEncoder) {
		encoder.AppendString(ts.Local().Format(TimeFormat))
	}
	config.EncodeDuration = func(d time.Duration, encoder zapcore.PrimitiveArrayEncoder) {
		val := float64(d) / float64(time.Millisecond)
		encoder.AppendString(fmt.Sprintf("%.3fms", val))
	}
	config.LevelKey = "lvl"
	return config
}

func nextID() string {
	return gen.NextString()
}

func newEncoder(format string) (zapcore.Encoder, error) {
	config := newEncoderConfig()
	switch format {
	case "json":
		return zapcore.NewJSONEncoder(config), nil
	case "console":
		return zapcore.NewConsoleEncoder(config), nil
	case "logfmt":
		return zaplogfmt.NewEncoder(config), nil
	default:
		return nil, fmt.Errorf("unknown logging format: %s", format)
	}
}

func createWriter(c *Config) io.Writer {
	if c.Dir != "" {
		dir := strings.TrimRight(c.Dir, string(filepath.Separator))
		return &lumberjack.Logger{
			Filename:   filepath.Join(dir, "metad.log"),
			MaxSize:    100,
			MaxBackups: 5,
			Compress:   true,
		}
	} else {
		return os.Stderr
	}
}

func InitialLogging(c *Config) (*zap.Logger, error) {
	encoder, err := newEncoder(c.Format)
	if err != nil {
		return nil, err
	}
	lvl := zap.InfoLevel
	switch strings.ToLower(c.Level) {
	case "info":
		lvl = zap.InfoLevel
	case "warn", "warning":
		lvl = zap.WarnLevel
	case "debug":
		lvl = zap.DebugLevel
	case "fatal":
		lvl = zap.FatalLevel
	case "panic":
		lvl = zap.PanicLevel
	}
	return zap.New(zapcore.NewCore(
		encoder,
		zapcore.Lock(zapcore.AddSync(createWriter(c))),
		lvl,
	), zap.Fields(zap.String("log_id", nextID()))), nil
}
