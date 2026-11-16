package http

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMaxBodyRefusesBodiesOverTheLimit(t *testing.T) {
	serve := func(t *testing.T, size int) (int64, error) {
		t.Helper()
		var read int64
		var readErr error
		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			read, readErr = io.Copy(io.Discard, r.Body)
		})
		body := strings.NewReader(strings.Repeat("x", size))
		MaxBody(inner).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", body))
		return read, readErr
	}

	t.Run("at the limit", func(t *testing.T) {
		read, err := serve(t, MaxRequestBodySize)
		if err != nil || read != MaxRequestBodySize {
			t.Fatalf("a body at the limit read %d bytes, error %v", read, err)
		}
	})
	t.Run("one byte over", func(t *testing.T) {
		read, err := serve(t, MaxRequestBodySize+1)
		var maxErr *http.MaxBytesError
		if !errors.As(err, &maxErr) {
			t.Fatalf("a body over the limit = %v, want http.MaxBytesError", err)
		}
		if read != MaxRequestBodySize {
			t.Errorf("read %d bytes, want the limit %d", read, MaxRequestBodySize)
		}
	})
}
