// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package delayed

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var _ TaskScheduler = &RedisTaskScheduler{}

func TestNewRedisTaskScheduler(t *testing.T) {
	ts, err := NewRedisTaskScheduler("TestTaskScheduler", "127.0.0.1:6379", "")
	assert.NoError(t, err)
	assert.NotNil(t, ts)
}
