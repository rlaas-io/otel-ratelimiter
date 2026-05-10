// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimiterprocessor // import "github.com/rlaas-io/otel-ratelimiter"

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
)

// policyWatcher watches a policy file for changes and calls onChange on write.
// It uses fsnotify for immediate inotify/kqueue/ReadDirectoryChangesW events,
// with a polling fallback for network file systems (NFS, CIFS, Docker volumes)
// that do not propagate kernel-level file events.
type policyWatcher struct {
	filePath string
	interval time.Duration
	onChange func()
	logger   *zap.Logger
	watcher  *fsnotify.Watcher
}

// newPolicyWatcher creates a file watcher that calls onChange whenever the
// policy file is written. interval is the polling fallback period (default: 15s).
func newPolicyWatcher(filePath string, interval time.Duration, onChange func(), logger *zap.Logger) (*policyWatcher, error) {
	if interval <= 0 {
		interval = 15 * time.Second
	}

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("create fsnotify watcher: %w", err)
	}
	if err := w.Add(filePath); err != nil {
		w.Close()
		return nil, fmt.Errorf("add policy file %q to watcher: %w", filePath, err)
	}

	return &policyWatcher{
		filePath: filePath,
		interval: interval,
		onChange: onChange,
		logger:   logger,
		watcher:  w,
	}, nil
}

// run blocks until ctx is cancelled, watching for file changes.
// Run this in a dedicated goroutine; it returns when ctx is done.
func (pw *policyWatcher) run(ctx context.Context) {
	ticker := time.NewTicker(pw.interval)
	defer ticker.Stop()
	defer pw.watcher.Close()

	// Track the file's mod time so polling doesn't trigger spurious reloads.
	var lastModTime time.Time
	if info, err := os.Stat(pw.filePath); err == nil {
		lastModTime = info.ModTime()
	}

	checkChanged := func() {
		info, err := os.Stat(pw.filePath)
		if err != nil {
			pw.logger.Warn("Policy file stat failed", zap.String("file", pw.filePath), zap.Error(err))
			return
		}
		if info.ModTime().After(lastModTime) {
			lastModTime = info.ModTime()
			pw.logger.Info("Policy file changed, triggering reload", zap.String("file", pw.filePath))
			pw.onChange()
		}
	}

	for {
		select {
		case <-ctx.Done():
			return

		case event, ok := <-pw.watcher.Events:
			if !ok {
				return
			}
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) || event.Has(fsnotify.Rename) {
				// Re-subscribe in case the file was atomically swapped (rename write
				// pattern used by editors and config management tools).
				_ = pw.watcher.Add(pw.filePath)
				checkChanged()
			}

		case err, ok := <-pw.watcher.Errors:
			if !ok {
				return
			}
			pw.logger.Warn("File watcher error; falling back to polling",
				zap.String("file", pw.filePath), zap.Error(err))

		case <-ticker.C:
			// Polling fallback: catches changes on file systems where fsnotify
			// events are not delivered (network mounts, some container runtimes).
			checkChanged()
		}
	}
}
