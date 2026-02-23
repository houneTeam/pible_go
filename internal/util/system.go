package util

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"strings"
	"time"
)

func IsRoot() bool {
	return os.Geteuid() == 0
}

func HasSystemctl() bool {
	_, err := exec.LookPath("systemctl")
	return err == nil
}

func ServiceIsActive(ctx context.Context, name string) bool {
	name = strings.TrimSpace(name)
	if name == "" || !HasSystemctl() {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "systemctl", "is-active", name)
	var out bytes.Buffer
	cmd.Stdout = &out
	_ = cmd.Run()
	return strings.TrimSpace(out.String()) == "active"
}

func RestartService(ctx context.Context, name string) error {
	name = strings.TrimSpace(name)
	if name == "" || !HasSystemctl() {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "systemctl", "restart", name)
	return cmd.Run()
}
