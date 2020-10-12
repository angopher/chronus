package raftmeta

import (
	"fmt"

	"go.etcd.io/etcd/raft"
	"go.uber.org/zap"
)

type raftLoggerBridge struct {
	raft.Logger
	suger *zap.SugaredLogger
}

func newRaftLoggerBridge(logger *zap.Logger) *raftLoggerBridge {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &raftLoggerBridge{
		suger: logger.Sugar(),
	}
}

func (b *raftLoggerBridge) Debug(v ...interface{}) {
	b.suger.Debug(v)
}
func (b *raftLoggerBridge) Debugf(format string, v ...interface{}) {
	b.suger.Debug(fmt.Sprintf(format, v...))
}

func (b *raftLoggerBridge) Error(v ...interface{}) {
	b.suger.Error(v)
}
func (b *raftLoggerBridge) Errorf(format string, v ...interface{}) {
	b.suger.Error(fmt.Sprintf(format, v...))
}

func (b *raftLoggerBridge) Info(v ...interface{}) {
	b.suger.Info(v)
}
func (b *raftLoggerBridge) Infof(format string, v ...interface{}) {
	b.suger.Info(fmt.Sprintf(format, v...))
}

func (b *raftLoggerBridge) Warning(v ...interface{}) {
	b.suger.Warn(v)
}
func (b *raftLoggerBridge) Warningf(format string, v ...interface{}) {
	b.suger.Warn(fmt.Sprintf(format, v...))
}

func (b *raftLoggerBridge) Fatal(v ...interface{}) {
	b.suger.Fatal(v)
}
func (b *raftLoggerBridge) Fatalf(format string, v ...interface{}) {
	b.suger.Fatal(fmt.Sprintf(format, v...))
}

func (b *raftLoggerBridge) Panic(v ...interface{}) {
	b.suger.Panic(v)
}
func (b *raftLoggerBridge) Panicf(format string, v ...interface{}) {
	b.suger.Panic(fmt.Sprintf(format, v...))
}
