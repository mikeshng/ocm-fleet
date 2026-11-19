package kubelabels

import (
	"strings"
	"testing"
)

func TestValidKey(t *testing.T) {
	valid := []string{
		"app",
		"app.kubernetes.io/name",
		// The name part may carry uppercase; only the prefix is lower case.
		"Foo",
		"a.b_c-d",
		"123-abc",
		"example.com/123-abc",
		strings.Repeat("k", 63),
		"example.com/" + strings.Repeat("k", 63),
	}
	for _, key := range valid {
		if !ValidKey(key) {
			t.Errorf("ValidKey(%q) = false, want true", key)
		}
	}
	invalid := []string{
		"",
		"a/b/c",
		"/name",
		"example.com/",
		"bad key",
		"ok:name",
		strings.Repeat("k", 64),
	}
	for _, key := range invalid {
		if ValidKey(key) {
			t.Errorf("ValidKey(%q) = true, want false", key)
		}
	}
}

func TestValidValue(t *testing.T) {
	valid := []string{"", "x", "v1.2_3", "12345", strings.Repeat("v", 63)}
	for _, value := range valid {
		if !ValidValue(value) {
			t.Errorf("ValidValue(%q) = false, want true", value)
		}
	}
	invalid := []string{strings.Repeat("v", 64), "has space", "-x", "x-", "has\nnewline"}
	for _, value := range invalid {
		if ValidValue(value) {
			t.Errorf("ValidValue(%q) = true, want false", value)
		}
	}
}
