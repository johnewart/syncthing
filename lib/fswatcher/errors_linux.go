// Copyright (C) 2016 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// +build linux

package fswatcher

import (
	"syscall"
)

func isWatchesTooFew(err error) bool {
	if errno, ok := err.(syscall.Errno); ok && (errno == 24 || errno == 28) {
		return true
	}
	return false
}
