// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package delayed

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var _ TaskScheduler = &BoltTaskScheduler{}

func TestNewBoltTaskScheduler(t *testing.T) {
	ts, err := NewBoltTaskScheduler("/tmp/TestTaskScheduler.bolt") // TODO Unique per-test db
	assert.NoError(t, err)
	assert.NotNil(t, ts)
}
