// Copyright (C) 2014 Jakob Borg and Contributors (see the CONTRIBUTORS file).
// All rights reserved. Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package model

import "github.com/syncthing/syncthing/protocol"

// nodeActivity tracks the number of outstanding requests per node and can
// answer which node is least busy.
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
