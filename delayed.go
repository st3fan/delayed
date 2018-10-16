// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/

package delayed

import (
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack"
)

var (
	nameToConcreteType sync.Map // map[string]reflect.Type
	concreteTypeToName sync.Map // map[reflect.Type]string
)

func RegisterTask(task Task) {
	// TODO Panic on registration of pointer types

	rt := reflect.TypeOf(task).Elem()
	name := rt.String()

	log.Printf("MOO Name = %s", name)

	if rt.Name() != "" {
		if rt.PkgPath() == "" {
			name = rt.Name()
		} else {
			name = rt.PkgPath() + "." + rt.Name()
		}
	}

	nameToConcreteType.Store(name, rt)
	concreteTypeToName.Store(rt, name)
}

//

type Task interface {
	Call() error
}

type TaskScheduler interface {
	ScheduleTaskAfter(task Task, delay time.Duration) (string, error)
	ScheduleTaskAt(task Task, pit time.Time) (string, error)
	CancelTask(taskID string) error
	Start() error
	Stop() error
}

// scheduledTaskWrapper is the object we serialize to bytes and put in Bolt. It contains everything needed
// to bring a ScheduledTask back to life.
type scheduledTaskWrapper struct {
	ID          string
	TypeName    string
	EncodedTask []byte
	Time        time.Time
}

func newScheduledTaskWrapper(task Task, time time.Time) (scheduledTaskWrapper, error) {
	encodedTask, err := msgpack.Marshal(task)
	if err != nil {
		return scheduledTaskWrapper{}, err
	}

	return scheduledTaskWrapper{
		ID:          uuid.New().String(),
		TypeName:    reflect.TypeOf(task).Elem().String(),
		EncodedTask: encodedTask,
		Time:        time,
	}, nil
}

func (w *scheduledTaskWrapper) Unwrap() (Task, error) {
	concreteType, ok := nameToConcreteType.Load(w.TypeName)
	if !ok {
		return nil, fmt.Errorf("Cannot find registered type for <%s>", w.TypeName)
	}

	t, ok := concreteType.(reflect.Type)
	if !ok {
		return nil, fmt.Errorf("Registered type for <%s> is not a reflect.Type", w.TypeName)
	}

	v := reflect.New(t).Interface()

	task, ok := v.(Task)
	if !ok {
		return nil, fmt.Errorf("Instantiated object is not of type ScheduledTask")
	}

	if err := msgpack.Unmarshal(w.EncodedTask, task); err != nil {
		return nil, fmt.Errorf("Failed to decode task: %s", err)
	}

	return task, nil
}
