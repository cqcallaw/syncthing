// Copyright (C) 2014 Jakob Borg and Contributors (see the CONTRIBUTORS file).
// All rights reserved. Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package model

import (
	"path/filepath"
	"sync"

	"github.com/syncthing/syncthing/events"
	"github.com/syncthing/syncthing/protocol"
	"github.com/syncthing/syncthing/scanner"
)

// TODO: Deletes
// TODO: Directories
// TODO: Identical file shortcut

// A pullBlockState is passed to the puller routine for each block that needs
// to be fetched.
type pullBlockState struct {
	*sharedPullerState
	block protocol.BlockInfo
}

// A copyBlocksState is passed to copy routine if the file has blocks to be
// copied from the original.
type copyBlocksState struct {
	*sharedPullerState
	blocks []protocol.BlockInfo
}

var (
	activeNodes = make(nodeActivity)
)

// pullerIteration runs a single puller iteration, for the given repo. One
// puller iteration handles all files currently flagged as needed in the repo.
// The specified number of copier, puller and finisher routines are used. It's
// seldom efficient to use more than one copier routine, while multiple
// pullers are essential and multiple finishers may be useful (they are
// primarily CPU bound due to hashing).
func (m *Model) pullerIteration(repo string, ncopiers, npullers, nfinishers int) {
	m.rmut.Lock()
	dir := m.repoCfgs[repo].Directory
	m.rmut.Unlock()

	pullChan := make(chan pullBlockState)
	copyChan := make(chan copyBlocksState)
	finisherChan := make(chan *sharedPullerState)
	doneChan := make(chan struct{})

	var wg sync.WaitGroup

	for i := 0; i < ncopiers; i++ {
		wg.Add(1)
		go func() {
			// copierRoutine finishes when copyChan is closed
			m.copierRoutine(copyChan, finisherChan)
			wg.Done()
		}()
	}

	for i := 0; i < npullers; i++ {
		wg.Add(1)
		go func() {
			// pullerRoutine finishes when pullChan is closed
			m.pullerRoutine(pullChan, finisherChan)
			wg.Done()
		}()
	}

	for i := 0; i < nfinishers; i++ {
		// finisherRoutine finishes when finisherChan is closed
		go func() {
			m.finisherRoutine(finisherChan)
			wg.Done()
		}()
	}

	m.rmut.RLock()
	files := m.repoFiles[repo]
	m.rmut.RUnlock()

	files.WithNeed(protocol.LocalNodeID, func(intf protocol.FileIntf) bool {
		file := intf.(protocol.FileInfo)

		switch {
		case protocol.IsDirectory(file.Flags) && protocol.IsDeleted(file.Flags):
			// A deleted directory
		case protocol.IsDirectory(file.Flags):
			// A new or changed directory
		case protocol.IsDeleted(file.Flags):
			// A deleted file
		default:
			// A new or changedfile
			m.handleFile(repo, dir, file, copyChan, pullChan)
		}

		return true
	})

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

// handleFile queues the copies and pulls as necessary for a single new or
// changed file.
func (m *Model) handleFile(repo, dir string, file protocol.FileInfo, copyChan chan<- copyBlocksState, pullChan chan<- pullBlockState) {
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
		for _, block := range need {
			ps := pullBlockState{
				sharedPullerState: &s,
				block:             block,
			}
			pullChan <- ps
		}
	}
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

		buf := make([]byte, scanner.StandardBlockSize)
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
		buf, err := m.Request(selected, state.repo, state.file.Name, state.block.Offset, int(state.block.Size))
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
		_, err = fd.WriteAt(buf, state.block.Offset)
		if err != nil {
			state.earlyClose("save", err)
			continue nextBlock
		}

		state.pullDone()
		out <- state.sharedPullerState
	}
}

func (m *Model) finisherRoutine(in <-chan *sharedPullerState) {
	for state := range in {
		if state.isDone() {
			err := state.finalClose()
			if err != nil {
				l.Warnln("puller: final:", err)
			}
			m.updateLocal(state.repo, state.file)
		}
	}
}
