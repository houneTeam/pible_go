package util

import (
	"fmt"
	"time"
)

const (
	ColorReset  = "\033[0m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorCyan   = "\033[36m"
	ColorGray   = "\033[90m"
)

func TimeHM() string {
	return time.Now().Format("15:04")
}

func Colorize(s string, color string) string {
	if color == "" {
		return s
	}
	return color + s + ColorReset
}

// Line prints a single console line prefixed with HH:MM.
func Line(label string, labelColor string, msg string) {
	if label != "" {
		fmt.Printf("%s %s %s\n", TimeHM(), Colorize(label, labelColor), msg)
		return
	}
	fmt.Printf("%s %s\n", TimeHM(), msg)
}

func Linef(label string, labelColor string, format string, args ...any) {
	Line(label, labelColor, fmt.Sprintf(format, args...))
}
