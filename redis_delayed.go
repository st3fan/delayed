// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package delayed

import (
	"log"
	"strconv"
	"time"

	"github.com/go-redis/redis"
	"github.com/st3fan/task"
	"github.com/vmihailenco/msgpack"
)

type RedisTaskScheduler struct {
	name       string
	client     *redis.Client
	moverTask  *task.Task
	workerTask *task.Task
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

	// We put two things in Redis, first the encodedWrapper as a regular Redis
	// item, keyed by its ID. Then we put the key in a sorted set, with the
	// execution time as the score.

	_, err = rts.client.TxPipelined(func(pipe redis.Pipeliner) error {
		pipe.Set("Delayed:"+rts.name+":"+wrapper.ID, encodedWrapper, 0) // TODO Probably add expirationt time?
		pipe.ZAdd("Delayed:"+rts.name, redis.Z{Member: wrapper.ID, Score: float64(wrapper.Time.Unix())})
		return nil
	})

	if err != nil {
		return "", err
	}

	return wrapper.ID, nil
}

func (rts *RedisTaskScheduler) CancelTask(taskID string) error {
	return nil // TODO
}

func (rts *RedisTaskScheduler) Start() error {
	rts.moverTask = task.New(rts.mover)
	rts.workerTask = task.New(rts.worker)
	return nil // TODO
}

func (rts *RedisTaskScheduler) Stop() error {
	rts.moverTask.SignalAndWait()
	rts.moverTask = nil
	rts.workerTask.SignalAndWait()
	rts.workerTask = nil
	return nil
}

func (rts *RedisTaskScheduler) worker(task *task.Task) {
	ticker := time.NewTicker(time.Second * 1)
	for {
		//log.Println("Worker: Outer loop")
		select {
		case <-task.HasBeenClosed():
			//log.Println("Worker: We have been closed")
			return
		case <-ticker.C:
			//log.Println("Worker: You got to work it")

			result, err := rts.client.BLPop(time.Second, "Delayed:"+rts.name+":Queue").Result()
			if err != nil {
				if err != redis.Nil {
					log.Println("Worker: LPop failed:", err)
				}
				continue // TODO This continues the select, not the for?
			}

			taskID := result[1]
			//log.Println("Worker: Popped", taskID)

			encodedWrapper, err := rts.client.Get("Delayed:" + rts.name + ":" + taskID).Bytes()
			if err != nil {
				log.Println("Worker: Get failed:", err)
				continue
			}

			var wrapper scheduledTaskWrapper
			if err := msgpack.Unmarshal(encodedWrapper, &wrapper); err != nil {
				log.Println("Worker: Failed to unmarshal task wrapper:", err)
				continue
			}

			task, err := wrapper.Unwrap()
			if err != nil {
				log.Printf("Worker: Failed to unwrap task: %s", err)
				continue
			}

			//log.Println("Worker: Going to call task ", taskID)

			if err := task.Call(); err != nil {
				log.Printf("Worker: Failed to execute task: %s", err)
			}

			if err := rts.client.Del("Delayed:" + rts.name + ":" + taskID).Err(); err != nil {
				log.Println("Worker: Del failed:", err)
				continue
			}
		}
	}
}

func (rts *RedisTaskScheduler) mover(task *task.Task) {
	ticker := time.NewTicker(time.Second * 1)
	for {
		select {
		case <-task.HasBeenClosed():
			//log.Println("Mover: We have been closed")
			return
		case <-ticker.C:
			//log.Println("Mover: I like to move it move it") // TODO This should become a Lua script

			elements, err := rts.client.ZRangeByScoreWithScores("Delayed:"+rts.name, redis.ZRangeBy{Min: "0", Max: strconv.FormatInt(time.Now().Unix(), 10)}).Result()
			if err != nil {
				log.Println("Mover: ZRangeByScoreWithScores failed:", err)
				continue
			}

			for _, e := range elements {
				//log.Println("Mover: Got ", e.Member, " ", e.Score)

				// Remove from the set
				if err := rts.client.ZRem("Delayed:"+rts.name, e.Member).Err(); err != nil {
					log.Println("Mover: ZRem failed:", err)
					continue
				}

				// Push the ID into the work queue
				if err := rts.client.RPush("Delayed:"+rts.name+":Queue", e.Member).Err(); err != nil {
					log.Println("Mover: RPush failed:", err)
					continue
				}
			}
		}
	}
}
