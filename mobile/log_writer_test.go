package mobile

import (
	"log"
	"strings"
	"testing"
)

type testLogWriter struct {
	message string
}

func (writer *testLogWriter) WriteLog(message string) {
	writer.message += message
}

func TestSetLogWriter(t *testing.T) {
	previousOutput := log.Writer()
	t.Cleanup(func() { log.SetOutput(previousOutput) })

	writer := &testLogWriter{}
	New().SetLogWriter(writer)
	log.Print("mobile logging is connected")

	if !strings.Contains(writer.message, "mobile logging is connected") {
		t.Fatalf("log message = %q", writer.message)
	}
}
