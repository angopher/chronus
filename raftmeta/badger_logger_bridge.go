package raftmeta

import (
	"strings"

	"github.com/dgraph-io/badger/v2"
	"go.uber.org/zap"
)

type badgerLoggerBridge struct {
	badger.Logger
	suger *zap.SugaredLogger
}

func NewBadgerLoggerBridge(logger *zap.Logger) *badgerLoggerBridge {
	return &badgerLoggerBridge{
		suger: logger.Sugar(),
	}
}

func (b *badgerLoggerBridge) Errorf(format string, args ...interface{}) {
	b.suger.Errorf(strings.Trim(format, "\n"), args...)
}

func (b *badgerLoggerBridge) Infof(format string, args ...interface{}) {
	b.suger.Infof(strings.Trim(format, "\n"), args...)
}

func (b *badgerLoggerBridge) Warningf(format string, args ...interface{}) {
	b.suger.Warnf(strings.Trim(format, "\n"), args...)
}

func (b *badgerLoggerBridge) Debugf(format string, args ...interface{}) {
	b.suger.Debugf(strings.Trim(format, "\n"), args...)
}
