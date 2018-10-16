// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package delayed

import (
	"bytes"
	"log"
	"time"

	"github.com/boltdb/bolt"
	"github.com/vmihailenco/msgpack"
)

type BoltTaskScheduler struct {
	db *bolt.DB
}

func NewBoltTaskScheduler(path string) (*BoltTaskScheduler, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("Tasks"))
		return err
	})
	if err != nil {
		return nil, err
	}

	return &BoltTaskScheduler{
		db: db,
	}, nil
}

func (t *BoltTaskScheduler) ScheduleTaskAfter(task Task, delay time.Duration) (string, error) {
	return t.ScheduleTaskAt(task, time.Now().Add(delay))
}

func (t *BoltTaskScheduler) ScheduleTaskAt(task Task, pit time.Time) (string, error) {
	wrapper, err := newScheduledTaskWrapper(task, pit)
	if err != nil {
		return "", err
	}

	encodedWrapper, err := msgpack.Marshal(wrapper)
	if err != nil {
		return "", err
	}

	key := []byte(wrapper.Time.Format(time.RFC3339))

	err = t.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("Tasks")).Put(key, encodedWrapper)
	})
	if err != nil {
		return "", err
	}

	return wrapper.ID, nil
}

func (t *BoltTaskScheduler) CancelTask(taskID string) error {
	return nil
}

func (t *BoltTaskScheduler) Start() error {
	go t.taskMover()
	go t.pollTasks()
	return nil
}

func (t *BoltTaskScheduler) taskMover() {
	// lastTimestampSeen := time.Time{}
	// for {
	// 	ticker := time.NewTicker(time.Second * 5)
	// 	select {
	// 	case <-ticker.C:
	// 		log.Println("Looking for tasks to execute")
	// 		t.db.Update(func(tx *bolt.Tx) error {
	// 			c := tx.Bucket([]byte("Tasks")).Cursor()

	// 			min := []byte(lastTimestampSeen.Format(time.RFC3339))
	// 			max := []byte(time.Now().Format(time.RFC3339))

	// 			for k, v := c.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, v = c.Next() {
	// 				// TODO move to Queue
	// 			}

	// 			return nil
	// 		})
	// 	}
	// }
}

func (t *BoltTaskScheduler) pollTasks() {
	lastTimestampSeen := time.Time{}
	for {
		ticker := time.NewTicker(time.Second * 5)
		select {
		case <-ticker.C:
			log.Println("Looking for tasks to execute")
			t.db.Update(func(tx *bolt.Tx) error {
				c := tx.Bucket([]byte("Tasks")).Cursor()

				min := []byte(lastTimestampSeen.Format(time.RFC3339))
				max := []byte(time.Now().Format(time.RFC3339))

				for k, v := c.Seek(min); k != nil && bytes.Compare(k, max) <= 0; k, v = c.Next() {

					var wrapper scheduledTaskWrapper
					if err := msgpack.Unmarshal(v, &wrapper); err != nil {
						log.Println("Failed to unmarshal task wrapper:", err)
					}

					log.Printf("Processing Task: %s/%s scheduled for %s at %s\n",
						wrapper.ID, wrapper.TypeName, wrapper.Time.Format(time.RFC3339), time.Now().Format(time.RFC3339))

					task, err := wrapper.Unwrap()
					if err != nil {
						log.Printf("Failed to unwrap task")
						continue
					}

					if err := task.Call(); err != nil {
						log.Printf("Failed to execute task: %s", err)
					}

					c.Delete()
				}

				return nil
			})
		}
	}
}

func (t *BoltTaskScheduler) Stop() error {
	if t.db != nil {
		err := t.db.Close()
		t.db = nil
		return err
	}
	return nil
}

var _ TaskScheduler = &BoltTaskScheduler{}
