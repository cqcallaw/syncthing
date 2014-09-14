// Copyright (C) 2014 Jakob Borg and Contributors (see the CONTRIBUTORS file).
// All rights reserved. Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package model

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/syncthing/syncthing/protocol"
)

// A sharedPullerState is kept for each file that is being synced and is kept
// updated along the way.
type sharedPullerState struct {
	// Immutable, does not require locking
	repo     string
	file     protocol.FileInfo
	tempName string
	realName string

	// Mutable, must be locked for access
	err        error      // The first error we hit
	fd         *os.File   // The fd of the temp file
	copyNeeded int        // Number of copy actions we expect to happen
	pullNeeded int        // Number of block pulls we expect to happen
	mut        sync.Mutex // Protects the above
}

// tempFile returns the fd for the temporary file, reusing an open fd
// or creating the file as necessary.
func (s *sharedPullerState) tempFile() (*os.File, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	// If we've already hit an error, return early
	if s.err != nil {
		return nil, s.err
	}

	// If the temp file is already open, return the file descriptor
	if s.fd != nil {
		return s.fd, nil
	}

	// Ensure that the parent directory exists or can be created
	dir := filepath.Dir(s.tempName)
	if info, err := os.Stat(dir); err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			s.earlyCloseLocked("dst mkdir", err)
			return nil, err
		}
	} else if err != nil {
		s.earlyCloseLocked("dst stat dir", err)
		return nil, err
	} else if !info.IsDir() {
		err = fmt.Errorf("%q: not a directory", dir)
		s.earlyCloseLocked("dst mkdir", err)
		return nil, err
	}

	// Attempt to create the temp file
	fd, err := os.Create(s.tempName)
	if err != nil {
		s.earlyCloseLocked("dst create", err)
		return nil, err
	}

	// Same fd will be used by all writers
	s.fd = fd

	return fd, nil
}

// sourceFile opens the existing source file for reading
func (s *sharedPullerState) sourceFile() (*os.File, error) {
	s.mut.Lock()
	defer s.mut.Unlock()

	// If we've already hit an error, return early
	if s.err != nil {
		return nil, s.err
	}

	// Attempt to open the existing file
	fd, err := os.Open(s.realName)
	if err != nil {
		s.earlyCloseLocked("src open", err)
		return nil, err
	}

	return fd, nil
}

// earlyClose prints a warning message composed of the context and
// error, and marks the sharedPullerState as failed. Is a no-op when called on
// an already failed state.
func (s *sharedPullerState) earlyClose(context string, err error) {
	s.mut.Lock()
	s.earlyCloseLocked(context, err)
	s.mut.Unlock()
}

func (s *sharedPullerState) earlyCloseLocked(context string, err error) {
	if s.err != nil {
		return
	}

	l.Infof("puller (%q / %q): %s: %v", s.repo, s.file.Name, context, err)
	s.err = err
	if s.fd != nil {
		s.fd.Close()
		os.Remove(s.tempName)
	}
}

func (s *sharedPullerState) failed() error {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.err
}

func (s *sharedPullerState) copyDone() {
	s.mut.Lock()
	s.copyNeeded--
	s.mut.Unlock()
}

func (s *sharedPullerState) pullDone() {
	s.mut.Lock()
	s.pullNeeded--
	s.mut.Unlock()
}

func (s *sharedPullerState) isDone() bool {
	s.mut.Lock()
	defer s.mut.Unlock()
	return s.pullNeeded+s.copyNeeded == 0
}

func (s *sharedPullerState) finalClose() error {
	return nil
}
