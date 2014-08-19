package sqlg

import (
    "fmt"
    //    "os"
    //    "path/filepath"
    //"runtime"
    "time"
)

var LogColors = map[VerboseLevel]int{
    VL_DEBUG: 102,
    VL_INFO:  28,
    VL_WARN:  214,
    VL_ERROR: 196,
}

const TIME_FORMAT = "2006-01-02T15:04:05.000000"

func toColorize(c int, s string) string {
    return fmt.Sprintf("\033[38;5;%dm%s\033[0m", c, s)
}

type VerboseLevel int

const (
    VL_DEBUG VerboseLevel = iota
    VL_INFO
    VL_WARN
    VL_ERROR
)

var LogPrefixes = map[VerboseLevel]string{
    VL_DEBUG: "DEBUG",
    VL_INFO:  "INFO ",
    VL_WARN:  "WARN ",
    VL_ERROR: "ERROR",
}

type Logger struct {
    LogLevel VerboseLevel
    Prefix   string
    Colorize bool
}

func closeLogger(log *Logger) {
    log.Close()
}

func NewLogger(logLevel VerboseLevel, prefix string, colorize bool) *Logger {
    log := &Logger{LogLevel: logLevel, Prefix: prefix, Colorize: colorize}
    // TODO doesn't work for some reasons
    //runtime.SetFinalizer(log, closeLogger)
    return log
}

func (l *Logger) Close() {
    l.Debug("Close logger")
    //    f, _ := os.Create("asd")
    //    f.WriteString(l.Prefix)
    //    f.Close()
}

func (l *Logger) Debugf(format string, n ...interface{}) {
    l.Logf(VL_DEBUG, format, n...)
}

func (l *Logger) Infof(format string, n ...interface{}) {
    l.Logf(VL_INFO, format, n...)
}

func (l *Logger) Warnf(format string, n ...interface{}) {
    l.Logf(VL_WARN, format, n...)
}

func (l *Logger) Errorf(format string, n ...interface{}) {
    l.Logf(VL_ERROR, format, n...)
}

func (l *Logger) Logf(level VerboseLevel, s string, n ...interface{}) {
    if level >= l.LogLevel {
        l.printLn(level, fmt.Sprintf(s, n...))
    }
}

func (l *Logger) Debug(n ...interface{}) {
    l.Log(VL_DEBUG, n...)
}

func (l *Logger) Info(n ...interface{}) {
    l.Log(VL_INFO, n...)
}

func (l *Logger) Warn(n ...interface{}) {
    l.Log(VL_WARN, n...)
}

func (l *Logger) Error(n ...interface{}) {
    l.Log(VL_ERROR, n...)
}

func (l *Logger) LogPrefix(level VerboseLevel, colorize bool) (s string) {
    s = time.Now().Format(TIME_FORMAT)
    if l.Prefix != "" {
        s = s + " [" + l.Prefix +
            //"/" + fmt.Sprintf("%v", l.LogLevel) +
            "]"
    }
    s = s + " " + l.LogLevelPrefix(level, colorize)
    return
}

func (l *Logger) LogLevelPrefix(level VerboseLevel, colorize bool) (s string) {
    prefix := LogPrefixes[level]
    if colorize {
        color := LogColors[level]
        return toColorize(color, prefix)
    } else {
        return prefix
    }
}

func (l *Logger) Log(level VerboseLevel, n ...interface{}) {
    if level >= l.LogLevel {
        s := fmt.Sprint(n...)
        l.printLn(level, s)
    }
}

func (l *Logger) stdoutPrintLn(level VerboseLevel, s string) {
    fmt.Println(l.LogPrefix(level, l.Colorize), s)
}

func (l *Logger) printLn(level VerboseLevel, s string) {
    l.stdoutPrintLn(level, s)
}
