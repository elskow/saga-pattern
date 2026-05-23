package logging

import (
	"io"
	"log/slog"
	"strings"
)

func NewTextLogger(writer io.Writer, level string) *slog.Logger {
	if writer == nil {
		writer = io.Discard
	}
	var programLevel slog.Level
	if err := programLevel.UnmarshalText([]byte(strings.ToUpper(strings.TrimSpace(level)))); err != nil {
		programLevel = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(writer, &slog.HandlerOptions{Level: programLevel}))
}
