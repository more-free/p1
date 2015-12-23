package server

// TODO move state to worker_proxy, make scheduler a stateless function
// there is no reason to maintain two different worker states (one in scheduler and one in workerProxy)

import (
	"github.com/cmu440/bitcoin"
	"time"
)

// a simple stateful scheduler interface
type Scheduler interface {
	AddWorker(id WorkerID)
	RemoveWorker(id WorkerID)
	GetAssignment(task *Task) []*TaskAssign // it splits a task to multiple TaskAssign
	SetTaskStarted(id WorkerID, task *Task)
	SetTaskFinished(id WorkerID, task *Task)
}

// a simple scheduler which splits tasks based on worker efficiency (task/time),
// which tries to achieve approximately-even distribution across all requests
type FairScheduler struct {
	workers   map[WorkerID]*workerHistory
	startTime map[WorkerID]int64
}

func NewFairScheduler() Scheduler {
	return &FairScheduler{
		workers:   make(map[WorkerID]*workerHistory),
		startTime: make(map[WorkerID]int64),
	}
}

func (s *FairScheduler) AddWorker(id WorkerID) {
	s.workers[id] = newWorkerHistory()
}

func (s *FairScheduler) RemoveWorker(id WorkerID) {
	delete(s.workers, id)
}

func (s *FairScheduler) SetTaskStarted(id WorkerID, task *Task) {
	s.startTime[id] = s.nowInSeconds()
}

func (s *FairScheduler) SetTaskFinished(id WorkerID, task *Task) {
	start := s.startTime[id]
	stop := s.nowInSeconds()
	delete(s.startTime, id)

	history := s.workers[id]
	history.totalTime += stop - start
	history.totalWorkload += task.Request.Upper - task.Request.Lower
}

func (s *FairScheduler) nowInSeconds() int64 {
	return time.Now().Unix() / 1000
}

func (s *FairScheduler) GetAssignment(task *Task) []*TaskAssign {
	assignments := make([]*TaskAssign, 0)
	if len(s.workers) == 0 {
		return assignments
	}

	requestID := task.ID
	totalWorkLoad := task.Request.Upper - task.Request.Lower

	// get total efficiency
	var totalEfficiencies float32 = 0.0
	for _, wh := range s.workers {
		totalEfficiencies += wh.getEfficiency()
	}

	// assign to the first n - 1 out of n with regards to their efficiency scores
	workers := make([]WorkerID, 0)
	for id, _ := range s.workers {
		workers = append(workers, id)
	}

	lower := task.Request.Lower
	for _, id := range workers[:len(workers)-1] {
		efficiency := s.workers[id].getEfficiency()
		ratio := efficiency / totalEfficiencies
		workload := uint64(float32(totalWorkLoad) * ratio)
		upper := lower + workload - 1
		assign := NewTaskAssign(id, NewTask(requestID, bitcoin.SubRequest(lower, upper, task.Request)))
		assignments = append(assignments, assign)
		lower = upper + 1
	}

	// assign the remaining workload to the last worker
	assign := NewTaskAssign(workers[len(workers)-1],
		NewTask(requestID, bitcoin.SubRequest(lower, task.Request.Upper, task.Request)))
	assignments = append(assignments, assign)

	return assignments
}

// TODO values may overflow
type workerHistory struct {
	totalTime     int64 // seconds
	totalWorkload uint64
}

func newWorkerHistory() *workerHistory {
	return &workerHistory{
		totalTime:     0,
		totalWorkload: 0,
	}
}

func (h *workerHistory) getEfficiency() float32 {
	return float32(h.totalWorkload) / float32(h.totalTime)
}
