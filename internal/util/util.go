package util

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

var (
	macRe = regexp.MustCompile(`^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$`)
)

func IsMACAddress(s string) bool {
	return macRe.MatchString(strings.TrimSpace(s))
}

func PromptString(prompt string) (string, error) {
	fmt.Print(prompt)
	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

func PromptInt(prompt string, defaultVal int) (int, error) {
	s, err := PromptString(prompt)
	if err != nil {
		return 0, err
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return defaultVal, nil
	}
	var v int
	_, err = fmt.Sscanf(s, "%d", &v)
	if err != nil {
		return defaultVal, nil
	}
	return v, nil
}

func NowTimestamp() string {
	return time.Now().Format("2006-01-02 15:04:05")
}

func BytesToHex(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	const hexdigits = "0123456789abcdef"
	out := make([]byte, 0, len(b)*3-1)
	for i, v := range b {
		if i > 0 {
			out = append(out, ' ')
		}
		out = append(out, hexdigits[v>>4], hexdigits[v&0x0f])
	}
	return string(out)
}

func SafeName(localName string) string {
	name := strings.TrimSpace(localName)
	if name == "" {
		return "Unknown"
	}
	if IsMACAddress(name) {
		return "Unknown"
	}
	return name
}
