// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package delayed

import (
	"time"

	"github.com/go-redis/redis"
	"github.com/vmihailenco/msgpack"
)

type RedisTaskScheduler struct {
	name   string
	client *redis.Client
}

func NewRedisTaskScheduler(name string, addr string) (*RedisTaskScheduler, error) {
	return &RedisTaskScheduler{
		name:   name,
		client: redis.NewClient(&redis.Options{Addr: addr}),
	}, nil
}

func (rts *RedisTaskScheduler) ScheduleTaskAfter(task Task, delay time.Duration) (string, error) {
	return rts.ScheduleTaskAt(task, time.Now().Add(delay))
}

func (rts *RedisTaskScheduler) ScheduleTaskAt(task Task, pit time.Time) (string, error) {
	wrapper, err := newScheduledTaskWrapper(task, pit)
	if err != nil {
		return "", err
	}

	encodedWrapper, err := msgpack.Marshal(wrapper)
	if err != nil {
		return "", err
	}

	z := redis.Z{
		Member: encodedWrapper,
		Score:  float64(wrapper.Time.Unix()),
	}

	if err := rts.client.ZAdd("Delayed", z).Err(); err != nil {
		return "", err
	}

	return wrapper.ID, nil
}

func (rts *RedisTaskScheduler) CancelTask(taskID string) error {
	return nil // TODO
}

func (rts *RedisTaskScheduler) Start() error {
	return nil // TODO
}

func (rts *RedisTaskScheduler) Stop() error {
	return nil // TODO
}

var _ TaskScheduler = &RedisTaskScheduler{}
