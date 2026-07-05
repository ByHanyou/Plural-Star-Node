// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"bufio"
	"log"
	"net"
	"net/http"
	"time"
)

func (s *Server) trafficEnabled() bool {
	return s != nil && s.cfg != nil && s.cfg.VerboseTraffic
}

func (s *Server) trafficf(format string, args ...any) {
	if !s.trafficEnabled() {
		return
	}
	log.Printf("traffic: "+format, args...)
}

// TrafficLogger returns the server's optional verbose traffic logger.
func (s *Server) TrafficLogger() func(format string, args ...any) {
	return s.trafficf
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	return hj.Hijack()
}

func (r *statusRecorder) Flush() {
	if fl, ok := r.ResponseWriter.(http.Flusher); ok {
		fl.Flush()
	}
}

func (r *statusRecorder) Push(target string, opts *http.PushOptions) error {
	if p, ok := r.ResponseWriter.(http.Pusher); ok {
		return p.Push(target, opts)
	}
	return http.ErrNotSupported
}

func (r *statusRecorder) statusCode() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}

func (s *Server) trafficMiddleware(next http.Handler) http.Handler {
	if !s.trafficEnabled() {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		s.trafficf("api method=%s path=%s remote=%s status=%d duration=%s", r.Method, r.URL.Path, r.RemoteAddr, rec.statusCode(), time.Since(start).Round(time.Millisecond))
	})
}
