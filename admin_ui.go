// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimiterprocessor // import "github.com/rlaas-io/otel-ratelimiter"

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed ui
var adminUIEmbed embed.FS

// adminUIHandler returns an http.Handler that serves the embedded admin UI
// static files. Mount it with an http.StripPrefix so paths inside the UI
// resolve correctly:
//
//	mux.Handle("GET /ui/", http.StripPrefix("/ui", adminUIHandler()))
func adminUIHandler() http.Handler {
	sub, err := fs.Sub(adminUIEmbed, "ui")
	if err != nil {
		panic("ratelimiter: admin UI embed misconfigured: " + err.Error())
	}
	return http.FileServer(http.FS(sub))
}
