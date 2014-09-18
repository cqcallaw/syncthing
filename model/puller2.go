// Copyright (C) 2014 Jakob Borg and Contributors (see the CONTRIBUTORS file).
// All rights reserved. Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package model

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/syncthing/syncthing/config"
	"github.com/syncthing/syncthing/events"
	"github.com/syncthing/syncthing/osutil"
	"github.com/syncthing/syncthing/protocol"
	"github.com/syncthing/syncthing/scanner"
)

// TODO: Directories
// TODO: Identical file shortcut
// TODO: Stop on errors
// TODO: Versioning

const (
	copiersPerRepo   = 1
	pullersPerRepo   = 16
	finishersPerRepo = 2
)

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
	activity  = newNodeActivity()
	errNoNode = errors.New("no available source node")
)

func (m *Model) runPuller(repo string, stopChan <-chan struct{}) {
	if debug {
		l.Debugln("starting puller/scanner for", repo)
	}

	m.rmut.Lock()
	repoCfg := m.repoCfgs[repo]
	m.rmut.Unlock()

	pullTicker := time.Tick(time.Second)
	scanTicker := time.Tick(time.Duration(repoCfg.RescanIntervalS) * time.Second)

	var prevVer uint64

loop:
	for {
		select {
		case <-stopChan:
			break loop

		case <-pullTicker:
			curVer := m.LocalVersion(repo)
			if curVer != prevVer {
				if debug {
					l.Debugln("pull", repo)
				}
				m.setState(repo, RepoSyncing)
				// TODO: Grab magic values from somewhere else
				changed := 1
				for changed > 0 {
					changed = m.pullerIteration(repo, copiersPerRepo, pullersPerRepo, finishersPerRepo)
					if debug {
						l.Debugln("pull", repo, "changed", changed)
					}
				}
				prevVer = curVer
				m.setState(repo, RepoIdle)
			}

		case <-scanTicker:
			if debug {
				l.Debugln("rescan", repo)
			}
			m.setState(repo, RepoScanning)
			if err := m.ScanRepo(repo); err != nil {
				invalidateRepo(m.cfg, repo, err)
				break loop
			}
			m.setState(repo, RepoIdle)
		}
	}

	// TODO: Should there be an actual RepoStopped state?
	m.setState(repo, RepoIdle)
	if debug {
		l.Debugln("stopping puller/scanner for", repo)
	}
}

// pullerIteration runs a single puller iteration for the given repo and
// returns the number of changed items. One puller iteration handles all files
// currently flagged as needed in the repo. The specified number of copier,
// puller and finisher routines are used. It's seldom efficient to use more
// than one copier routine, while multiple pullers are essential and multiple
// finishers may be useful (they are primarily CPU bound due to hashing).
func (m *Model) pullerIteration(repo string, ncopiers, npullers, nfinishers int) int {
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

	changed := 0
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

		changed++
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

	return changed
}

// handleDir creates or updates the given directory
func (m *Model) handleDir(repo, dir string, file protocol.FileInfo) {
	realName := filepath.Join(dir, file.Name)
	mode := os.FileMode(file.Flags & 0777)

	if debug {
		curFile := m.CurrentRepoFile(repo, file.Name)
		l.Debugf("need dir\n\t%v\n\t%v", file, curFile)
	}

	var err error
	if info, err := os.Stat(realName); err != nil && os.IsNotExist(err) {
		err = os.MkdirAll(realName, mode)
	} else if !info.IsDir() {
		l.Infof("pull (%q / %q): should be dir, but is not", repo, file.Name)
		return
	} else {
		err = os.Chmod(realName, mode)
	}

	if err == nil {
		m.updateLocal(repo, file)
	}
}

// deleteDir attempts to delete the given directory
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
		err = os.Chmod(realDir, 0755)
		if err == nil {
			defer func() {
				err = os.Chmod(realDir, info.Mode())
				if err != nil {
					panic(err)
				}
			}()
		}
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

	if debug {
		l.Debugf("need file\n\t%v\n\t%v", file, curFile)
	}

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

			// Set the correct permission bits on the new file
			err = os.Chmod(state.tempName, os.FileMode(state.file.Flags&0777))
			if err != nil {
				os.Remove(state.tempName)
				l.Warnln("puller: final:", err)
				continue
			}

			// Set the correct timestamp on the new file
			t := time.Unix(state.file.Modified, 0)
			err = os.Chtimes(state.tempName, t, t)
			if err != nil {
				os.Remove(state.tempName)
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

func invalidateRepo(cfg *config.Configuration, repoID string, err error) {
	for i := range cfg.Repositories {
		repo := &cfg.Repositories[i]
		if repo.ID == repoID {
			repo.Invalid = err.Error()
			return
		}
	}
}
