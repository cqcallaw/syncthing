// Copyright (C) 2014 Jakob Borg and Contributors (see the CONTRIBUTORS file).
// All rights reserved. Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package model

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/syncthing/syncthing/protocol"
	"github.com/syncthing/syncthing/scanner"
)

type segment struct {
	offset, size int64
}

type request struct {
	global   protocol.FileInfo // target (global) FileInfo
	tempFile *os.File          // fd of temporary file
	blocks   []segment         // blocks to copy or pull
	abort    chan struct{}     // abort signal to all workers
}

type result struct {
	global   protocol.FileInfo // target (global) FileInfo
	tempFile *os.File          // fd of temporary file
	err      error             // error while processing the request, or nil
}

const (
	pullBatchSize = 100
)

/*

queueBlocks  ->  1 * copier
             ->  n * puller  ->  closer

*/

func queuer(copy, pull chan<- request, done <-chan struct{}) {
	var prevVer uint64
	for {
		time.Sleep(5 * time.Second)

		curVer := p.model.LocalVersion(p.repoCfg.ID)
		if curVer == prevVer {
			continue
		}

		if debug {
			l.Debugf("%q: checking for more needed blocks", p.repoCfg.ID)
		}

		// We grab up to pullBatchSize files from the database. We limit the
		// number of files to conserve memory, but also need to grab a
		// nontrivial amount so the order can be randomized.

		files := make([]protocol.FileInfo, 0, pullBatchSize)
		for _, f := range p.model.NeedFilesRepo(p.repoCfg.ID) {
			// TODO: Avoid enqueing files already in the pipeline?
			files = append(files, f)
		}

		// We enqueue the files in random order to improve sync efficiency
		// with multiple nodes

		perm := rand.Perm(len(files))
		for _, idx := range perm {
			f := files[idx]
			lf := p.model.CurrentRepoFile(p.repoCfg.ID, f.Name)
			have, need := scanner.BlockDiff(lf.Blocks, f.Blocks)

			tempFile, err := openTemp(f)
			if err != nil {
				// TODO: handle elegantly
				panic(err)
			}

			abortChan := make(chan struct{})

			copy <- request{
				tempFile: tempFile,
				global:   f,
				blocks:   have,
				abort:    abortChan,
			}

			pull <- request{
				tempFile: tempFile,
				global:   f,
				blocks:   need,
				abort:    abortChan,
			}
		}
	}
}

// Handles requests by copying data from an existing source file
func copier(reqs <-chan request, res chan<- result) {

}

// Handles requests by requesting data from the network
func puller(reqs chan request, res chan result) {

}

// An abortable file request
func doRequest() {

}

func tempName(f protocol.FileInfo) string {
	return filepath.Join(filepath.Dir(f.Name), fmt.Sprintf(".syncthing.%s.%d", filepath.Base(f.Name), f.Version))
}

func openTemp(f protocol.FileInfo) (*os.File, error) {
	return nil, errors.New("not implemented")
}
