package server

import (
	"container/list"
	"fmt"
	"github.com/cmu440/bitcoin"
	"math"
	"time"
)

type RuntimeMap map[int]WorkerRunTime

type TaskStart struct {
	WorkerID int
	Request  *bitcoin.Message
}

type TaskDone struct {
	ClientID int
	Result   *bitcoin.Message // global result
}

type Task struct {
	ID        int              // unique task id
	ClientID  int              // client conn ID
	ParentID  int              // parent task id, or -1 if no parent task
	WorkerID  int              // assigned to which worker, or -1 if not assigned as a whole
	Request   *bitcoin.Message // the request message
	Result    *bitcoin.Message // the result message
	Subtasks  *bitcoin.IntSet  // empty if no sub-task
	StartTime time.Time
	StopTime  time.Time
}

func NewParentTask(id int, clientID int, req *bitcoin.Message) *Task {
	return &Task{
		ID:       id,
		ClientID: clientID,
		ParentID: -1,
		WorkerID: -1,
		Request:  req,
		Result:   nil,
		Subtasks: bitcoin.NewIntSet(),
	}
}

func NewSubTask(id int, clientID int, parentID int, workerID int, req *bitcoin.Message) *Task {
	return &Task{
		ID:       id,
		ClientID: clientID,
		ParentID: parentID,
		WorkerID: workerID,
		Request:  req,
		Result:   nil,
		Subtasks: bitcoin.NewIntSet(),
	}
}

func (t *Task) SetStarted() {
	t.StartTime = time.Now()
}

func (t *Task) SetStopped() {
	t.StopTime = time.Now()
}

func (t *Task) GetRunningTime() time.Duration {
	return t.StopTime.Sub(t.StartTime)
}

func (t *Task) AddSubTask(id int) {
	t.Subtasks.Add(id)
}

func (t *Task) AddResult(res *bitcoin.Message) {
	t.Result = res
}

func (t *Task) HasResult() bool {
	return t.Result != nil
}

func (t *Task) HasParent() bool {
	return t.ParentID != -1
}

func (t *Task) HasSubtask() bool {
	return t.Subtasks.Len() > 0
}

func (t *Task) GetSubtasks() []int {
	return t.Subtasks.Keys()
}

func (t *Task) AssignTo(workerID int) {
	t.WorkerID = workerID
}

func (t *Task) RemoveSubTask(subid int) {
	t.Subtasks.Remove(subid)
}

/*
This interface will be used by the scheduler to achieve a fair strategy
*/
type WorkerRunTime interface {
	WorkLoad() float32
	WorkTime() int64
	SetWorkLoad(float32)
	SetWorkTime(int64)

	// add later
	// StartTime() time.Time
	// RunningTime() time.Duration
	// CPU() float32
	// Memory() float32
	// DiskIO() float32
	// NetworkIO() float32
}

// a naive implementation used for bit miner only
type WorkerHistory struct {
	workLoad    float32 // accumulated upper - lower
	workTime    int64   // nano seconds
	lastStarted time.Time
}

func NewWorkerHistory() WorkerRunTime {
	return &WorkerHistory{
		workLoad: 0.0,
		workTime: 0,
	}
}

// note it may return 0 for newly joined workers
func (w *WorkerHistory) WorkLoad() float32 {
	return w.workLoad
}

func (w *WorkerHistory) SetWorkLoad(wl float32) {
	w.workLoad = wl
}

func (w *WorkerHistory) WorkTime() int64 {
	return w.workTime
}

func (w *WorkerHistory) SetWorkTime(wt int64) {
	w.workTime = wt
}

type WorkerProxy interface {
	HandleRequest(req *bitcoin.Message, clientID int)
	HandleResult(res *bitcoin.Message, workerID int)
	HandleWorkerLost(workerID int) // connection lost or any failure
	HandleWorkerJoin(workerID int)
}

type workerProxy struct {
	scheduler     Scheduler
	runtime       RuntimeMap    // worker id -> workerRunTime
	tasks         map[int]*Task // task id -> Task
	taskStartChan chan<- *TaskStart
	taskDoneChan  chan<- *TaskDone
	pendingReq    *resMsgQueue
	nextTaskID    int // used internally
}

func NewWorkerProxy(scheduler Scheduler, taskStartChan chan<- *TaskStart, taskDoneChan chan<- *TaskDone) WorkerProxy {
	return &workerProxy{
		scheduler:     scheduler,
		taskStartChan: taskStartChan,
		taskDoneChan:  taskDoneChan,
		tasks:         make(map[int]*Task),
		runtime:       make(map[int]WorkerRunTime),
		pendingReq:    NewResMsgQueue(),
		nextTaskID:    1,
	}
}

func (w *workerProxy) HandleRequest(req *bitcoin.Message, clientID int) {
	w.handleRequest(req, clientID, -1)
}

func (w *workerProxy) handleRequest(req *bitcoin.Message, clientID int, parentID int) {
	if w.getWorkerSize() == 0 {
		w.pendingReq.Push(req, clientID)
		return
	}

	var parentTask *Task
	if parentID <= 0 {
		parentID = w.getNextTaskID()
		parentTask = NewParentTask(parentID, clientID, req)
		w.addTask(parentID, parentTask)
	} else {
		parentTask = w.getTask(parentID)
	}

	subtasks := make([]*Task, 0)
	assigns := w.scheduler.Map(req, w.runtime)
	for _, assign := range assigns {
		subid := w.getNextTaskID()
		assign.request.SetID(subid)
		subtask := NewSubTask(subid, clientID, parentID, assign.workerID, assign.request)
		w.addTask(subid, subtask)
		parentTask.AddSubTask(subid)
		subtasks = append(subtasks, subtask)
		subtask.SetStarted() // ideally it should be invoked by server only after a request is sent to worker successfully
	}

	w.sendRequest(subtasks)
}

func (w *workerProxy) HandleResult(res *bitcoin.Message, workerID int) {
	subid := res.GetID()

	// ignore non-existing task result. this may be caused by a worker connection lost :
	// imagine a worker gets a request and then lost connection, then the server will delete the original tasks
	// and reassign them to other workers. However the failed worker recovers soon and send back a result of the
	// original task, which is deleted. In this case we need to ignore the result.
	if !w.taskExists(subid) {
		return
	}

	subtask := w.getTask(subid)
	subtask.AddResult(res)
	subtask.SetStopped()

	parentTask := w.getTask(subtask.ParentID)
	if w.isAllTaskFinished(parentTask) {
		result := w.scheduler.Reduce(w.getSubResults(parentTask))
		parentTask.AddResult(result)
		w.sendResult(parentTask)
		w.removeTask(parentTask)
	}
}

func (w *workerProxy) HandleWorkerLost(workerID int) {
	w.removeWorker(workerID)

	// it doesn't delete the parent task, instead, it removes all subtasks, create new subtasks with those
	// requests contained in the original subtasks, and reassign them to the parent task. As a result, for
	// a parent task, the only changing part is the Subtask field.
	for _, task := range w.removeSubTasksByWorker(workerID) {
		w.handleRequest(task.Request, task.ClientID, task.ParentID)
	}
}

func (w *workerProxy) HandleWorkerJoin(workerID int) {
	w.addWorker(workerID)

	// pop out all pending task and schedule it
	for {
		req, clientID, err := w.pendingReq.Pop()
		if err != nil {
			break
		}
		w.HandleRequest(req, clientID)
	}
}

func (w *workerProxy) getSubResults(parentTask *Task) []*bitcoin.Message {
	subresults := make([]*bitcoin.Message, 0)
	for _, subid := range parentTask.GetSubtasks() {
		subtask := w.getTask(subid)
		subresults = append(subresults, subtask.Result)
	}
	return subresults
}

func (w *workerProxy) removeSubTasksByWorker(workerID int) []*Task {
	removed := make([]*Task, 0)
	for _, task := range w.tasks {
		if task.WorkerID == workerID { // only subtasks will satisfy this condition
			w.removeTask(task)
			w.getTask(task.ParentID).RemoveSubTask(task.ID)
			removed = append(removed, task)
		}
	}
	return removed
}

func (w *workerProxy) getWorkerSize() int {
	return len(w.runtime)
}

func (w *workerProxy) getNextTaskID() int {
	id := w.nextTaskID
	w.nextTaskID = (id + 1) % math.MaxInt32
	return id
}

func (w *workerProxy) addTask(id int, task *Task) {
	w.tasks[id] = task
}

// remove subtask or parent task.  it also removes all subtasks given a parent task.
func (w *workerProxy) removeTask(task *Task) {
	id := task.ID
	delete(w.tasks, id)

	if task.HasSubtask() {
		for _, subid := range task.GetSubtasks() {
			delete(w.tasks, subid)
		}
	}
}

func (w *workerProxy) taskExists(id int) bool {
	_, exists := w.tasks[id]
	return exists
}

func (w *workerProxy) getTask(id int) *Task {
	return w.tasks[id]
}

func (w *workerProxy) getWorker(workerID int) WorkerRunTime {
	return w.runtime[workerID]
}

func (w *workerProxy) addWorker(workerID int) {
	w.runtime[workerID] = NewWorkerHistory()
}

// it does not remove tasks assigned to the worker
func (w *workerProxy) removeWorker(workerID int) {
	delete(w.runtime, workerID)
}

func (w *workerProxy) sendRequest(subtasks []*Task) {
	go func() {
		for _, subtask := range subtasks {
			w.taskStartChan <- &TaskStart{subtask.WorkerID, subtask.Request}
		}
	}()
}

func (w *workerProxy) sendResult(parentTask *Task) {
	go func() {
		w.taskDoneChan <- &TaskDone{parentTask.ClientID, parentTask.Result}
	}()
}

func (w *workerProxy) isAllTaskFinished(task *Task) bool {
	if !task.HasSubtask() {
		return task.HasResult()
	} else {
		done := true
		for _, subid := range task.GetSubtasks() {
			subtask := w.getTask(subid)
			if !subtask.HasResult() {
				done = false
				break
			}
		}
		return done
	}
}

// helper types
type resMsg struct {
	req      *bitcoin.Message
	clientID int
}

type resMsgQueue struct {
	queue *list.List
}

func NewResMsgQueue() *resMsgQueue {
	return &resMsgQueue{list.New()}
}

func (q *resMsgQueue) Push(req *bitcoin.Message, clientID int) {
	q.queue.PushBack(&resMsg{req, clientID})
}

func (q *resMsgQueue) Pop() (*bitcoin.Message, int, error) {
	if q.Len() <= 0 {
		return nil, 0, fmt.Errorf("pop from empty queue")
	}

	top := q.queue.Front()
	q.queue.Remove(top)
	topMsg := top.Value.(*resMsg)
	return topMsg.req, topMsg.clientID, nil
}

func (q *resMsgQueue) Len() int {
	return q.queue.Len()
}
