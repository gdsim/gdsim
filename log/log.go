package log

import (
	"fmt"
	"log"
)

type Level int

const (
	FATAL Level = iota
	ERROR
	WARN
	INFO
	DEBUG
)

type Logger struct {
	context string
	level   Level
}

func (l Logger) Fatalf(format string, v ...interface{}) {
	compound := fmt.Sprintf("FATAL: %s: %v", l.context, format)
	if l.level <= FATAL {
		log.Fatalf(compound, v...)
	}
}

func (l Logger) Errorf(format string, v ...interface{}) {
	compound := fmt.Sprintf("ERROR: %s: %v", l.context, format)
	if l.level <= ERROR {
		log.Printf(compound, v...)
	}
}

func (l Logger) Warnf(format string, v ...interface{}) {
	compound := fmt.Sprintf("WARN: %s: %v", l.context, format)
	if l.level <= WARN {
		log.Printf(compound, v...)
	}
}

func (l Logger) Infof(format string, v ...interface{}) {
	compound := fmt.Sprintf("INFO: %s: %v", l.context, format)
	if l.level <= INFO {
		log.Printf(compound, v...)
	}
}

func (l Logger) Debugf(format string, v ...interface{}) {
	compound := fmt.Sprintf("DEBUG: %s: %v", l.context, format)
	if l.level <= DEBUG {
		log.Printf(compound, v...)
	}
}
