package main

import (
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestRun_RejectsBadInvocations(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cases := map[string]struct {
		args []string
		want string
	}{
		"no flags":         {nil, "--server-url"},
		"cleartext server": {[]string{"--server-url=http://fleet.example", "--bundle-token-file=/t", "--service-account=sa"}, "https"},
		"no bundle token":  {[]string{"--server-url=https://fleet.example", "--service-account=sa"}, "--bundle-token-file"},
		"stray argument":   {[]string{"--server-url=https://fleet.example", "--bundle-token-file=/t", "--service-account=sa", "extra"}, "unexpected argument"},
		"unknown flag":     {[]string{"--no-such-flag"}, "not defined"},
		"missing CA file":  {[]string{"--server-url=https://fleet.example", "--bundle-token-file=/t", "--service-account=sa", "--server-ca-file=/nonexistent/ca.crt"}, "--server-ca-file"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := run(tc.args, logger)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("run(%v) = %v, want an error naming %q", tc.args, err, tc.want)
			}
		})
	}
}
