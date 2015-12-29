package server

import (
	"github.com/cmu440/bitcoin"
	"log"
)

// a simple stateful scheduler interface
type Scheduler interface {
	GetAssignment(task *Task, rt map[WorkerID]WorkerRunTime) []*TaskAssign // it splits a task to multiple TaskAssign
}

// a simple scheduler which splits tasks based on worker efficiency (task/time),
// which tries to achieve approximately-even distribution across all requests
type FairScheduler struct {
}

func NewFairScheduler() Scheduler {
	return &FairScheduler{}
}

func (s *FairScheduler) getTaskRatioByEfficiency(rts map[WorkerID]WorkerRunTime) map[WorkerID]float32 {
	ratio := make(map[WorkerID]float32)

	// case 1, if all workers are new, return a even ratio
	allZero := true
	for _, rt := range rts {
		if rt.WorkTime() != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		avg := 1.0 / float32(len(rts))
		for id, _ := range rts {
			ratio[id] = avg
		}
		return ratio
	}

	// if some of workers are new. assign avg efficiency score to them, then do the calibration
	var totalEfficiency float32 = 0.0
	effectiveWorkers := 0
	for id, rt := range rts {
		if rt.WorkTime() != 0 {
			ef := rt.WorkLoad() / float32(rt.WorkTime())
			ratio[id] = ef
			totalEfficiency += ef
			effectiveWorkers += 1
		}
	}
	avgEfficiency := totalEfficiency / float32(effectiveWorkers)
	for id, rt := range rts {
		if rt.WorkTime() == 0 {
			ratio[id] = avgEfficiency
			totalEfficiency += avgEfficiency
		}
	}

	// calibration - make the sum of all efficiency score to 1
	for id, ef := range ratio {
		ratio[id] = ef / totalEfficiency
	}

	return ratio
}

func (s *FairScheduler) GetAssignment(task *Task, rts map[WorkerID]WorkerRunTime) []*TaskAssign {
	assignments := make([]*TaskAssign, 0)
	if len(rts) == 0 {
		return assignments
	}

	requestID := task.ID
	totalWorkLoad := task.Request.Upper - task.Request.Lower
	efficiencyRatio := s.getTaskRatioByEfficiency(rts)
	log.Println("current efficiency ratio :", efficiencyRatio)

	// assign to the first n - 1 out of n with regards to their efficiency scores
	workers := make([]WorkerID, 0)
	for id, _ := range rts {
		workers = append(workers, id)
	}

	lower := task.Request.Lower
	for _, id := range workers[:len(workers)-1] {
		ratio := efficiencyRatio[id]
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

	s.printAssignments(assignments)
	return assignments
}

func (s *FairScheduler) printAssignments(assignments []*TaskAssign) {
	for _, assign := range assignments {
		log.Println(assign.ID, " <- ", assign.Task.Request)
	}
}
