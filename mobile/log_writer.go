package mobile

import (
	"log"
	"os"
)

// LogWriter receives internal olcRTC log messages.
type LogWriter interface {
	WriteLog(msg string)
}

// SetLogWriter redirects the process-wide Go logger used by olcRTC.
// Passing nil restores stderr output.
func (r *Runtime) SetLogWriter(writer LogWriter) {
	if writer == nil {
		log.SetOutput(os.Stderr)
		return
	}
	log.SetOutput(logBridge{writer: writer})
}

type logBridge struct {
	writer LogWriter
}

func (bridge logBridge) Write(message []byte) (int, error) {
	bridge.writer.WriteLog(string(message))
	return len(message), nil
}
