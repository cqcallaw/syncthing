// Copyright (C) 2014 Jakob Borg and Contributors (see the CONTRIBUTORS file).
// All rights reserved. Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package model

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/syncthing/syncthing/events"
	"github.com/syncthing/syncthing/protocol"
	"github.com/syncthing/syncthing/scanner"
)

// TODO: Deletes
// TODO: Directories
// TODO: Identical file shortcut

type pullBlockState struct {
	*sharedPullerState
	blockNo int
}

type copyBlocksState struct {
	*sharedPullerState
	blocks []protocol.BlockInfo
}

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

type nodeActivity map[protocol.NodeID]int

func (m nodeActivity) leastBusy(availability []protocol.NodeID) protocol.NodeID {
	var low int = 2<<30 - 1
	var selected protocol.NodeID
	for _, node := range availability {
		if usage := m[node]; usage < low {
			low = usage
			selected = node
		}
	}
	return selected
}

func (m nodeActivity) using(node protocol.NodeID) {
	m[node]++
}

func (m nodeActivity) done(node protocol.NodeID) {
	m[node]--
}

var (
	activeNodes = make(nodeActivity)
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, scanner.StandardBlockSize)
	},
}

func (m *Model) pullerIteration(repo, dir string) {
	pullChan := make(chan pullBlockState)
	copyChan := make(chan copyBlocksState)
	finisherChan := make(chan *sharedPullerState)
	doneChan := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		// copierRoutine finishes when copyChan is closed
		m.copierRoutine(copyChan, finisherChan)
		wg.Done()
	}()

	go func() {
		// pullerRoutine finishes when pullChan is closed
		m.pullerRoutine(pullChan, finisherChan)
		wg.Done()
	}()

	// finisherRoutine finishes when finisherChan is closed, and closes
	// doneChan
	go m.finisherRoutine(finisherChan, doneChan)

	var neededFiles []protocol.FileInfo
	// get needed files

	for _, file := range neededFiles {
		events.Default.Log(events.ItemStarted, map[string]string{
			"repo": repo,
			"item": file.Name,
		})

		tempName := filepath.Join(dir, defTempNamer.TempName(file.Name))
		realName := filepath.Join(dir, file.Name)

		s := sharedPullerState{
			repo:     repo,
			file:     file,
			tempName: tempName,
			realName: realName,
		}

		curFile := m.CurrentRepoFile(repo, file.Name)
		have, need := scanner.BlockDiff(curFile.Blocks, file.Blocks)

		if len(have) > 0 {
			s.copyNeeded = 1
			cs := copyBlocksState{
				sharedPullerState: &s,
				blocks:            have,
			}
			copyChan <- cs
		}

		if len(need) > 0 {
			s.pullNeeded = len(need)
			for i := range need {
				ps := pullBlockState{
					sharedPullerState: &s,
					blockNo:           i,
				}
				pullChan <- ps
			}
		}
	}

	// Signal copy and puller routines that we are done with the in data for
	// this iteration
	close(copyChan)
	close(pullChan)

	// Wait for them to finish, then signal the finisher chan that there will
	// be no more input.
	wg.Wait()
	close(finisherChan)

	// Wait for the finisherChan to finish.
	<-doneChan
}

// copierRoutine reads pullerStates until the in channel closes, checks each
// state for blocks that we already have, and performs the relevant copy.
func (m *Model) copierRoutine(in <-chan copyBlocksState, out chan<- *sharedPullerState) {
nextFile:
	for state := range in {
		dstFd, err := state.tempFile()
		if err != nil {
			// Nothing more to do for this failed file (the error was logged
			// when it happened)
			continue nextFile
		}

		srcFd, err := state.sourceFile()
		if err != nil {
			// As above
			continue nextFile
		}

		buf := bufferPool.Get().([]byte)
		for _, block := range state.blocks {
			buf = buf[:int(block.Size)]

			_, err = srcFd.ReadAt(buf, block.Offset)
			if err != nil {
				state.earlyClose("src read", err)
				continue nextFile
			}

			_, err = dstFd.WriteAt(buf, block.Offset)
			if err != nil {
				state.earlyClose("dst write", err)
				continue nextFile
			}

			state.copyDone()
			out <- state.sharedPullerState
		}
		bufferPool.Put(buf[:cap(buf)])
	}
}

func (m *Model) pullerRoutine(in <-chan pullBlockState, out chan<- *sharedPullerState) {
nextBlock:
	for state := range in {
		if state.failed() != nil {
			continue nextBlock
		}

		// Select the least busy node to pull the block from. If we found no
		// feasible node at all, fail the block (and in the long run, the
		// file).
		potentialNodes := m.availability(state.repo, state.file.Name)
		selected := activeNodes.leastBusy(potentialNodes)
		if selected == (protocol.NodeID{}) {
			state.earlyClose("pull", errNoNode)
			continue nextBlock
		}

		// Fetch the block, while marking the selected node as in use so that
		// leastBusy can select another node when someone else asks.
		activeNodes.using(selected)
		block := state.file.Blocks[state.blockNo]
		buf, err := m.Request(selected, state.repo, state.file.Name, block.Offset, int(block.Size))
		activeNodes.done(selected)
		if err != nil {
			state.earlyClose("pull", err)
			continue nextBlock
		}

		// Save the block data we got from the cluster
		fd, err := state.tempFile()
		if err != nil {
			continue nextBlock
		}
		_, err = fd.WriteAt(buf, block.Offset)
		if err != nil {
			state.earlyClose("save", err)
			continue nextBlock
		}

		state.pullDone()
		out <- state.sharedPullerState
	}
}

func (m *Model) finisherRoutine(in <-chan *sharedPullerState, done chan struct{}) {
	for state := range in {
		if state.isDone() {
			err := state.finalClose()
			if err != nil {
				l.Warnln("puller: final:", err)
			}
			m.updateLocal(state.repo, state.file)
		}
	}
	close(done)
}

// ---

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

	l.Infof("puller (%s / %q): %s: %v", s.repo, s.file.Name, context, err)
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
