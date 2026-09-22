/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package app

import (
	"flag"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/ingress-gce/pkg/systemhealth"
	"k8s.io/klog/v2"
)

func TestLoopbackOnly(t *testing.T) {
	okHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("multitenancy_metrics_ok"))
	})
	wrapped := loopbackOnly(okHandler)

	testCases := []struct {
		name           string
		remoteAddr     string
		wantStatusCode int
		wantBodySubstr string
	}{
		{
			name:           "IPv4 loopback 127.0.0.1 is allowed",
			remoteAddr:     "127.0.0.1:54321",
			wantStatusCode: http.StatusOK,
			wantBodySubstr: "multitenancy_metrics_ok",
		},
		{
			name:           "IPv4 loopback 127.0.0.2 is allowed",
			remoteAddr:     "127.0.0.2:8087",
			wantStatusCode: http.StatusOK,
			wantBodySubstr: "multitenancy_metrics_ok",
		},
		{
			name:           "IPv6 loopback ::1 is allowed",
			remoteAddr:     "[::1]:54321",
			wantStatusCode: http.StatusOK,
			wantBodySubstr: "multitenancy_metrics_ok",
		},
		{
			name:           "External IPv4 is forbidden",
			remoteAddr:     "10.128.0.5:54321",
			wantStatusCode: http.StatusForbidden,
			wantBodySubstr: "Forbidden: accessed from outside localhost",
		},
		{
			name:           "External IPv6 is forbidden",
			remoteAddr:     "[2001:db8::1]:54321",
			wantStatusCode: http.StatusForbidden,
			wantBodySubstr: "Forbidden: accessed from outside localhost",
		},
		{
			name:           "Missing port in RemoteAddr is forbidden",
			remoteAddr:     "127.0.0.1",
			wantStatusCode: http.StatusForbidden,
			wantBodySubstr: "Forbidden: accessed from outside localhost",
		},
		{
			name:           "Invalid IP in RemoteAddr is forbidden",
			remoteAddr:     "not-an-ip:8087",
			wantStatusCode: http.StatusForbidden,
			wantBodySubstr: "Forbidden: accessed from outside localhost",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/metrics/multitenancy", nil)
			req.RemoteAddr = tc.remoteAddr
			rec := httptest.NewRecorder()

			wrapped.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatusCode {
				t.Errorf("loopbackOnly(%q) status = %d, want %d", tc.remoteAddr, rec.Code, tc.wantStatusCode)
			}
			if body := rec.Body.String(); !strings.Contains(body, tc.wantBodySubstr) {
				t.Errorf("loopbackOnly(%q) body = %q, want substring %q", tc.remoteAddr, body, tc.wantBodySubstr)
			}
		})
	}
}

func TestHTTPServerEndpointsAccessControl(t *testing.T) {
	if flag.Lookup("v") == nil {
		klog.InitFlags(nil)
	}
	mux := http.NewServeMux()
	checker := func() systemhealth.HealthCheckResults {
		return systemhealth.HealthCheckResults{"neg": nil}
	}
	mux.HandleFunc("/healthz", healthCheckHandler(checker, klog.TODO()))
	mux.HandleFunc("/flag", flagHandler)
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/metrics/multitenancy", loopbackOnly(promhttp.Handler()))

	testCases := []struct {
		name           string
		path           string
		remoteAddr     string
		wantStatusCode int
	}{
		{
			name:           "/healthz allows external non-loopback requests",
			path:           "/healthz",
			remoteAddr:     "10.128.0.5:54321",
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "/healthz allows loopback requests",
			path:           "/healthz",
			remoteAddr:     "127.0.0.1:54321",
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "/metrics allows external non-loopback requests",
			path:           "/metrics",
			remoteAddr:     "10.128.0.5:54321",
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "/metrics allows loopback requests",
			path:           "/metrics",
			remoteAddr:     "127.0.0.1:54321",
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "/metrics/multitenancy rejects external non-loopback requests",
			path:           "/metrics/multitenancy",
			remoteAddr:     "10.128.0.5:54321",
			wantStatusCode: http.StatusForbidden,
		},
		{
			name:           "/metrics/multitenancy allows loopback requests",
			path:           "/metrics/multitenancy",
			remoteAddr:     "127.0.0.1:54321",
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "/flag allows external non-loopback requests",
			path:           "/flag",
			remoteAddr:     "10.128.0.5:54321",
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "/flag allows loopback requests",
			path:           "/flag",
			remoteAddr:     "127.0.0.1:54321",
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			req.RemoteAddr = tc.remoteAddr
			rec := httptest.NewRecorder()

			mux.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatusCode {
				t.Errorf("GET %s from %s returned status %d, want %d", tc.path, tc.remoteAddr, rec.Code, tc.wantStatusCode)
			}
		})
	}
}
