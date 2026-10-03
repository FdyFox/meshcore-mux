package mux

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// Level is a log severity, selected through the LOG_LEVEL environment variable.
type Level int

const (
	LevelTrace Level = iota
	LevelDebug
	LevelInfo
	LevelWarn
	LevelError
	LevelNone
)

var levelNames = [...]string{"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "NONE"}

// Logger is a tiny leveled logger writing one line per record to stderr.
// Callers guard expensive messages (such as wire dumps) with Enabled.
type Logger struct {
	mu    sync.Mutex
	level Level
	out   io.Writer
}

// Log is the process-wide logger.
var Log = &Logger{level: LevelInfo, out: os.Stderr}

// SetupLogFromEnv applies LOG_LEVEL (trace, debug, info, warn, error, none).
func SetupLogFromEnv() {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL"))) {
	case "trace":
		Log.SetLevel(LevelTrace)
	case "debug":
		Log.SetLevel(LevelDebug)
	case "warn", "warning":
		Log.SetLevel(LevelWarn)
	case "error", "fatal":
		Log.SetLevel(LevelError)
	case "none", "off":
		Log.SetLevel(LevelNone)
	default:
		Log.SetLevel(LevelInfo)
	}
}

// SetLevel changes the minimum logged severity.
func (l *Logger) SetLevel(lv Level) {
	l.mu.Lock()
	l.level = lv
	l.mu.Unlock()
}

// SetOutput redirects log output.
func (l *Logger) SetOutput(w io.Writer) {
	l.mu.Lock()
	l.out = w
	l.mu.Unlock()
}

// Enabled reports whether a record at lv would be written.
func (l *Logger) Enabled(lv Level) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return lv >= l.level && lv < LevelNone
}

// Logf writes one formatted record.
func (l *Logger) Logf(lv Level, format string, args ...any) {
	if !l.Enabled(lv) {
		return
	}
	msg := format
	if len(args) > 0 {
		msg = fmt.Sprintf(format, args...)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.out, "%s %-5s %s\n", time.Now().UTC().Format("2006-01-02T15:04:05.000000Z"), levelNames[lv], msg)
}

func (l *Logger) Debugf(format string, args ...any) { l.Logf(LevelDebug, format, args...) }
func (l *Logger) Infof(format string, args ...any)  { l.Logf(LevelInfo, format, args...) }
func (l *Logger) Warnf(format string, args ...any)  { l.Logf(LevelWarn, format, args...) }
func (l *Logger) Errorf(format string, args ...any) { l.Logf(LevelError, format, args...) }
