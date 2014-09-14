// Copyright (C) 2014 Jakob Borg and Contributors (see the CONTRIBUTORS file).
// All rights reserved. Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package model

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/syncthing/syncthing/events"
	"github.com/syncthing/syncthing/osutil"
	"github.com/syncthing/syncthing/protocol"
	"github.com/syncthing/syncthing/scanner"
)

// TODO: Directories
// TODO: Identical file shortcut
// TODO: Stop on errors

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

var activity = newNodeActivity()

// pullerIteration runs a single puller iteration for the given repo. One
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

	var wg sync.WaitGroup
	var doneWg sync.WaitGroup

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
		doneWg.Add(1)
		// finisherRoutine finishes when finisherChan is closed
		go func() {
			m.finisherRoutine(finisherChan)
			doneWg.Done()
		}()
	}

	m.rmut.RLock()
	files := m.repoFiles[repo]
	m.rmut.RUnlock()

	// !!!
	// WithNeed takes a database snapshot (by necessity). By the time we've
	// handled a bunch of files it might have become out of date and we might
	// be attempting to sync with an old version of a file...
	// !!!

	files.WithNeed(protocol.LocalNodeID, func(intf protocol.FileIntf) bool {
		file := intf.(protocol.FileInfo)

		switch {
		case protocol.IsDirectory(file.Flags) && protocol.IsDeleted(file.Flags):
			// A deleted directory
			m.deleteDir(repo, dir, file)
		case protocol.IsDirectory(file.Flags):
			// A new or changed directory
			m.handleDir(repo, dir, file)
		case protocol.IsDeleted(file.Flags):
			// A deleted file
			m.deleteFile(repo, dir, file)
		default:
			// A new or changed file
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
	doneWg.Wait()
}

// deleteDir attempts to delete the given directory
func (m *Model) handleDir(repo, dir string, file protocol.FileInfo) {
}

// handleDir creates or updates the given directory
func (m *Model) deleteDir(repo, dir string, file protocol.FileInfo) {
}

// deleteFile attempts to delete the given file
func (m *Model) deleteFile(repo, dir string, file protocol.FileInfo) {
	realName := filepath.Join(dir, file.Name)
	realDir := filepath.Dir(realName)
	if info, err := os.Stat(dir); err == nil && info.IsDir() && info.Mode()&04 == 0 {
		// A non-writeable directory (for this user; we assume that's the
		// relevant part). Temporarily change the mode so we can delete the
		// file inside it.
		os.Chmod(realDir, 0755)
		defer os.Chmod(realDir, info.Mode())
		err = os.Remove(realName)
		if err != nil {
			l.Infoln("puller (%s / %q): delete: %v", repo, file.Name, err)
		} else {
			m.updateLocal(repo, file)
		}
	}
}

// handleFile queues the copies and pulls as necessary for a single new or
// changed file.
func (m *Model) handleFile(repo, dir string, file protocol.FileInfo, copyChan chan<- copyBlocksState, pullChan chan<- pullBlockState) {
	events.Default.Log(events.ItemStarted, map[string]string{
		"repo": repo,
		"item": file.Name,
	})

	// Figure out the absolute filenames we need once and for all
	tempName := filepath.Join(dir, defTempNamer.TempName(file.Name))
	realName := filepath.Join(dir, file.Name)

	s := sharedPullerState{
		repo:     repo,
		file:     file,
		tempName: tempName,
		realName: realName,
	}

	curFile := m.CurrentRepoFile(repo, file.Name)
	copyBlocks, pullBlocks := scanner.BlockDiff(curFile.Blocks, file.Blocks)

	if len(copyBlocks) > 0 {
		s.copyNeeded = 1
		cs := copyBlocksState{
			sharedPullerState: &s,
			blocks:            copyBlocks,
		}
		copyChan <- cs
	}

	if len(pullBlocks) > 0 {
		s.pullNeeded = len(pullBlocks)
		for _, block := range pullBlocks {
			ps := pullBlockState{
				sharedPullerState: &s,
				block:             block,
			}
			pullChan <- ps
		}
	}
}

// copierRoutine reads pullerStates until the in channel closes and performs
// the relevant copy.
func (m *Model) copierRoutine(in <-chan copyBlocksState, out chan<- *sharedPullerState) {
	buf := make([]byte, scanner.StandardBlockSize)

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

		for _, block := range state.blocks {
			buf = buf[:int(block.Size)]

			_, err = srcFd.ReadAt(buf, block.Offset)
			if err != nil {
				state.earlyClose("src read", err)
				srcFd.Close()
				continue nextFile
			}

			_, err = dstFd.WriteAt(buf, block.Offset)
			if err != nil {
				state.earlyClose("dst write", err)
				srcFd.Close()
				continue nextFile
			}
		}

		srcFd.Close()
		state.copyDone()
		out <- state.sharedPullerState
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
		selected := activity.leastBusy(potentialNodes)
		if selected == (protocol.NodeID{}) {
			state.earlyClose("pull", errNoNode)
			continue nextBlock
		}

		// Fetch the block, while marking the selected node as in use so that
		// leastBusy can select another node when someone else asks.
		activity.using(selected)
		buf, err := m.requestGlobal(selected, state.repo, state.file.Name, state.block.Offset, int(state.block.Size), state.block.Hash)
		activity.done(selected)
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
				continue
			}

			// Verify the file against expected hashes
			fd, err := os.Open(state.tempName)
			if err != nil {
				l.Warnln("puller: final:", err)
				continue
			}
			err = scanner.Verify(fd, scanner.StandardBlockSize, state.file.Blocks)
			fd.Close()
			if err != nil {
				l.Warnln("puller: final:", err)
				continue
			}

			// Replace the original file with the new one
			err = osutil.Rename(state.tempName, state.realName)
			if err != nil {
				os.Remove(state.tempName)
				l.Warnln("puller: final:", err)
				continue
			}

			// Record the updated file in the index
			m.updateLocal(state.repo, state.file)
		}
	}
}
