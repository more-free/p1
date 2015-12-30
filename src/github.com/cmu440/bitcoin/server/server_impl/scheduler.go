package server

import (
	"github.com/cmu440/bitcoin"
	"log"
	"math"
)

type Assign struct {
	workerID int
	request  *bitcoin.Message
}

// a simple stateful scheduler interface
type Scheduler interface {
	Map(request *bitcoin.Message, rt RuntimeMap) []*Assign
	Reduce(subresults []*bitcoin.Message) *bitcoin.Message
}

// a simple scheduler which splits tasks based on worker efficiency (task/time),
// which tries to achieve approximately-even distribution across all requests
type FairScheduler struct {
}

func NewFairScheduler() Scheduler {
	return &FairScheduler{}
}

func (s *FairScheduler) Map(req *bitcoin.Message, rts RuntimeMap) []*Assign {
	assignments := make([]*Assign, 0)
	if len(rts) == 0 {
		return assignments
	}

	totalWorkLoad := req.Upper - req.Lower
	efficiencyRatio := s.getTaskRatioByEfficiency(rts)
	log.Println("current efficiency ratio :", efficiencyRatio)

	// assign to the first n - 1 out of n with regards to their efficiency scores
	workers := make([]int, 0)
	for id, _ := range rts {
		workers = append(workers, id)
	}

	lower := req.Lower
	for _, id := range workers[:len(workers)-1] {
		ratio := efficiencyRatio[id]
		workload := uint64(float32(totalWorkLoad) * ratio)
		upper := lower + workload - 1

		assign := &Assign{id, bitcoin.SubRequest(lower, upper, req)}
		assignments = append(assignments, assign)
		lower = upper + 1
	}

	// assign the remaining workload to the last worker
	assign := &Assign{workers[len(workers)-1], bitcoin.SubRequest(lower, req.Upper, req)}
	assignments = append(assignments, assign)

	s.printAssignments(assignments)
	return assignments
}

func (s *FairScheduler) Reduce(res []*bitcoin.Message) *bitcoin.Message {
	var nonce uint64
	var minHash uint64 = math.MaxUint64

	for _, r := range res {
		if r.Hash < minHash {
			minHash = r.Hash
			nonce = r.Nonce
		}
	}

	return bitcoin.NewResult(minHash, nonce)
}

func (s *FairScheduler) getTaskRatioByEfficiency(rts map[int]WorkerRunTime) map[int]float32 {
	ratio := make(map[int]float32)

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

func (s *FairScheduler) printAssignments(assignments []*Assign) {
	for _, assign := range assignments {
		log.Println(assign.workerID, " <- ", assign.request)
	}
}
