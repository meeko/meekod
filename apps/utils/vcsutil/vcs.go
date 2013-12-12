// Copyright (c) 2013 The cider AUTHORS
//
// Use of this source code is governed by The MIT License
// that can be found in the LICENSE file.

package vcsutil

import (
	"fmt"
	"github.com/cider/cider/apps"
	"net/url"
)

type VCS interface {
	Clone(repoURL *url.URL, srcDir string, ctx apps.ActionContext) error
	Pull(repoURL *url.URL, srcDir string, ctx apps.ActionContext) error
}

func GetVCS(scheme string) (VCS, error) {
	switch scheme {
	case "git+ssh":
		return newGitVCS("ssh"), nil
	case "git+https":
		return newGitVCS("https"), nil
	case "git+file":
		return newGitVCS("file"), nil
	default:
		return nil, fmt.Errorf("unknown vcs scheme: %s", scheme)
	}
}
