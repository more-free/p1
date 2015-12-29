package server

// TODO add stat and runtime to maintain worker metadata, stat, runtime info (dynamic), etc.
// these parameters can be used as input of the scheduler. Accordingly, scheduler should be
// stateless
// TODO if we are allowed to modify the structure of messages, this part can be much simpler (ex. adding an id
// to a message may eliminate most structures defined in this file)

import (
	"container/list"
	"fmt"
	"github.com/cmu440/bitcoin"
	"time"
)

type RequestID int
type WorkerID int

func ToRequestID(id int) RequestID {
	return RequestID(id)
}

func ToWorkerID(id int) WorkerID {
	return WorkerID(id)
}

func (i RequestID) ToInt() int {
	return int(i)
}

func (i WorkerID) ToInt() int {
	return int(i)
}

// a task is a unique request ID + a request message (might be a sub-requests divided by scheduler,
// which have the same request ID)
type Task struct {
	ID      RequestID
	Request *bitcoin.Message
}

func (t *Task) Equals(that *Task) bool {
	return t.ID == that.ID && bitcoin.RequestEquals(t.Request, that.Request)
}

func NewTask(id RequestID, request *bitcoin.Message) *Task {
	return &Task{
		ID:      id,
		Request: request,
	}
}

// a task assignment is a unique worker ID (in the project use connID) + a task
type TaskAssign struct {
	ID   WorkerID
	Task *Task
}

func (t *TaskAssign) Equals(that *TaskAssign) bool {
	return t.ID == that.ID && t.Task.Equals(that.Task)
}

func NewTaskAssign(id WorkerID, task *Task) *TaskAssign {
	return &TaskAssign{
		ID:   id,
		Task: task,
	}
}

// a task result is a unique request ID + a result message
// it is sent from server to client. Result field contains the
// global result by merging partial results from workers.
type TaskResult struct {
	ID     RequestID
	Result *bitcoin.Message
}

// the raw message sent back from workers
type TaskPartialResult struct {
	ID            WorkerID
	PartialResult *bitcoin.Message
}

func NewTaskPartialResult(id WorkerID, msg *bitcoin.Message) *TaskPartialResult {
	return &TaskPartialResult{
		ID:            id,
		PartialResult: msg,
	}
}

// a task table is a mapping of worker ID -> Task
type TaskTable struct {
	tasks map[WorkerID]*TaskQueue
}

func NewTaskTable() *TaskTable {
	return &TaskTable{
		tasks: make(map[WorkerID]*TaskQueue),
	}
}

func (t *TaskTable) AddWorker(id WorkerID) {
	t.tasks[id] = NewTaskQueue()
}

func (t *TaskTable) AddTask(id WorkerID, task *Task) {
	q, exists := t.tasks[id]
	if !exists {
		t.AddWorker(id)
		q = t.tasks[id]
	}
	q.Add(task)
}

func (t *TaskTable) GetTaskQueue(id WorkerID) *TaskQueue {
	return t.tasks[id]
}

func (t *TaskTable) GetTopTask(id WorkerID) (*Task, error) {
	return t.tasks[id].Top()
}

func (t *TaskTable) RemoveWorker(id WorkerID) {
	delete(t.tasks, id)
}

func (t *TaskTable) PopTask(id WorkerID) (*Task, error) {
	return t.tasks[id].Pop()
}

func (t *TaskTable) GetWorkers() []WorkerID {
	keys := make([]WorkerID, 0)
	for id, _ := range t.tasks {
		keys = append(keys, id)
	}
	return keys
}

// a request table is a mapping of request ID -> taskAssign
type RequestTable struct {
	requests map[RequestID]*TaskAssignSet
}

func NewRequestTable() *RequestTable {
	return &RequestTable{
		requests: make(map[RequestID]*TaskAssignSet),
	}
}

func (t *RequestTable) AddRequest(id RequestID) {
	t.requests[id] = NewTaskAssignSet()
}

func (t *RequestTable) RemoveRequest(id RequestID) {
	delete(t.requests, id)
}

func (t *RequestTable) GetTaskAssign(id RequestID) []*TaskAssign {
	return t.requests[id].GetAllAssign()
}

func (t *RequestTable) AddTaskAssign(id RequestID, taskAssign *TaskAssign) {
	s, exists := t.requests[id]
	if !exists {
		t.AddRequest(id)
		s = t.requests[id]
	}
	s.Add(taskAssign)
}

func (t *RequestTable) AddTaskResult(id RequestID, taskResult *TaskPartialResult) {
	s, exists := t.requests[id]
	if !exists {
		t.AddRequest(id)
		s = t.requests[id]
	}
	s.AddResult(taskResult)
}

func (t *RequestTable) GetTaskResult(id RequestID) []*TaskPartialResult {
	return t.requests[id].GetAllResult()
}

func (t *RequestTable) ContainsTaskAssign(id RequestID, taskAssign *TaskAssign) bool {
	return t.requests[id].Contains(taskAssign)
}

func (t *RequestTable) RemoveTaskAssign(id RequestID, taskAssign *TaskAssign) {
	t.requests[id].Remove(taskAssign)
}

func (t *RequestTable) RemoveAllTaskAssign(id WorkerID) {
	for _, rs := range t.requests {
		rs.RemoveTaskAssignByID(id)
	}
}

/*
This interface will be used by the scheduler to achieve a fair strategy
*/
type WorkerRunTime interface {
	WorkLoad() float32
	SetWorkLoad(float32)
	WorkTime() int64
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

func NewWorkerHistory() *WorkerHistory {
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

func (w *WorkerHistory) setLastStartTime(st time.Time) {
	w.lastStarted = st
}

func (w *WorkerHistory) getLastStartTime() time.Time {
	return w.lastStarted
}

/*
Due to the limit of pre-defined messages, we assume that
at anytime a worker is only working on one task, so that we don't need to explicitly encode task id to Request
and Result messages.
This can be improved by adding a unique ID field to message struct.
*/
type WorkerProxy interface {
	Schedule(*Task)
	HandlePartialResult(*TaskPartialResult)
	HandleWorkerLost(WorkerID) // connection lost or any failure
	HandleWorkerJoin(WorkerID)
}

type Reducer func(partial []*TaskPartialResult, id RequestID) *TaskResult

func NewReducer() Reducer {
	return func(partial []*TaskPartialResult, id RequestID) *TaskResult {
		hash := partial[0].PartialResult.Hash
		nonce := partial[0].PartialResult.Nonce
		for _, pr := range partial {
			if pr.PartialResult.Hash < hash {
				hash = pr.PartialResult.Hash
				nonce = pr.PartialResult.Nonce
			}
		}

		return &TaskResult{
			ID:     id,
			Result: bitcoin.NewResult(hash, nonce),
		}
	}
}

// all methods are supposed to be invoked in a single thread env, which doesn't introduce race condition
type OneForOneWorkerProxy struct {
	mapper       Scheduler
	reducer      Reducer
	taskTable    *TaskTable
	requestTable *RequestTable
	runtime      map[WorkerID]*WorkerHistory
	taskToStart  chan<- *TaskAssign
	taskResult   chan<- *TaskResult
	pendingTask  *TaskQueue // pending requests due to no connected worker
}

func NewOneForOneWorkerProxy(mapper Scheduler, reducer Reducer,
	taskToStart chan<- *TaskAssign, taskResult chan<- *TaskResult) WorkerProxy {
	return &OneForOneWorkerProxy{
		mapper:       mapper,
		reducer:      reducer,
		taskTable:    NewTaskTable(),
		requestTable: NewRequestTable(),
		runtime:      make(map[WorkerID]*WorkerHistory),
		taskToStart:  taskToStart,
		taskResult:   taskResult,
		pendingTask:  NewTaskQueue(),
	}
}

func (w *OneForOneWorkerProxy) notifyTaskStart(assign *TaskAssign) {
	go func() {
		w.taskToStart <- assign
	}()
}

func (w *OneForOneWorkerProxy) notifyTaskDone(result *TaskResult) {
	go func() {
		w.taskResult <- result
	}()
}

/*
 non-blocking
*/
func (w *OneForOneWorkerProxy) Schedule(newTask *Task) {
	if w.getWorkerSize() == 0 {
		w.pendingTask.Add(newTask)
		return
	}

	assignments := w.mapper.GetAssignment(newTask, w.getRuntimeMap())

	// update request table
	for _, assign := range assignments {
		requestID := assign.Task.ID
		w.requestTable.AddTaskAssign(requestID, assign)
	}

	// update task table
	for _, assign := range assignments {
		workerID := assign.ID
		w.taskTable.AddTask(workerID, assign.Task)

		// if no other running tasks, notify the taskToStart channel
		if w.taskTable.GetTaskQueue(workerID).Len() == 1 {
			w.notifyTaskStart(assign)
			w.runtime[workerID].setLastStartTime(time.Now())
		}
	}
}

func (w *OneForOneWorkerProxy) getRuntimeMap() map[WorkerID]WorkerRunTime {
	rts := make(map[WorkerID]WorkerRunTime)
	for id, rt := range w.runtime {
		rts[id] = rt
	}
	return rts
}

func (w *OneForOneWorkerProxy) addWorkerRuntime(id WorkerID) {
	w.runtime[id] = NewWorkerHistory()
}

func (w *OneForOneWorkerProxy) removeWorkerRuntime(id WorkerID) {
	delete(w.runtime, id)
}

func (w *OneForOneWorkerProxy) updateWorkerRuntime(id WorkerID, taskFinished *Task) {
	rt := w.runtime[id]
	rt.SetWorkLoad(rt.WorkLoad() + float32(taskFinished.Request.Upper-taskFinished.Request.Lower))
	duration := time.Since(rt.getLastStartTime()).Nanoseconds()
	rt.SetWorkTime(rt.WorkTime() + duration)
}

func (w *OneForOneWorkerProxy) getWorkerSize() int {
	return len(w.taskTable.GetWorkers())
}

func (w *OneForOneWorkerProxy) HandlePartialResult(partialResult *TaskPartialResult) {
	// get task instance of give partialResult from task table
	workerID := partialResult.ID
	finishedTask, _ := w.taskTable.PopTask(workerID)
	requestID := finishedTask.ID

	w.updateWorkerRuntime(workerID, finishedTask)

	// set result for request table
	w.requestTable.AddTaskResult(requestID, partialResult)

	// send message to taskResult channel if all sub-tasks of the request have been finished
	if len(w.requestTable.GetTaskAssign(requestID)) == len(w.requestTable.GetTaskResult(requestID)) {
		result := w.reducer(w.requestTable.GetTaskResult(requestID), requestID)
		w.notifyTaskDone(result)
		w.requestTable.RemoveRequest(requestID)
	}

	// update task table by starting the next task of workerID
	newTask, err := w.taskTable.GetTopTask(workerID)
	if err == nil { // if it has task left, start it
		w.notifyTaskStart(NewTaskAssign(workerID, newTask))
	}
}

// it re-assigns all tasks belonging to the failed worker to other workers
func (w *OneForOneWorkerProxy) HandleWorkerLost(id WorkerID) {
	w.removeWorkerRuntime(id)

	failedTasks := w.taskTable.GetTaskQueue(id).GetAll()
	w.taskTable.RemoveWorker(id)

	// remove failed tasks from request table
	w.requestTable.RemoveAllTaskAssign(id)

	// reschedule failed tasks
	for _, failedTask := range failedTasks {
		w.Schedule(failedTask)
	}
}

func (w *OneForOneWorkerProxy) HandleWorkerJoin(id WorkerID) {
	w.addWorkerRuntime(id)
	w.taskTable.AddWorker(id)

	for {
		task, err := w.pendingTask.Pop()
		if err == nil {
			w.Schedule(task)
		} else {
			break
		}
	}
}

// helper types
type TaskQueue struct {
	queue *list.List
}

func NewTaskQueue() *TaskQueue {
	return &TaskQueue{
		queue: list.New(),
	}
}

func (q *TaskQueue) GetAll() []*Task {
	tasks := make([]*Task, 0)
	for e := q.queue.Front(); e != nil; e = e.Next() {
		tasks = append(tasks, e.Value.(*Task))
	}
	return tasks
}

func (q *TaskQueue) Add(task *Task) {
	q.queue.PushBack(task)
}

func (q *TaskQueue) Pop() (*Task, error) {
	if q.Len() == 0 {
		return nil, fmt.Errorf("pop from empty queue")
	}

	front := q.queue.Front()
	q.queue.Remove(front)
	return front.Value.(*Task), nil
}

func (q *TaskQueue) Top() (*Task, error) {
	if q.Len() == 0 {
		return nil, fmt.Errorf("top from empty queue")
	}

	front := q.queue.Front()
	return front.Value.(*Task), nil
}

func (q *TaskQueue) Len() int {
	return q.queue.Len()
}

// TODO not really a set now
type assignAndResult struct {
	assign *TaskAssign
	result *TaskPartialResult
}

func newAssignAndResult(assign *TaskAssign) *assignAndResult {
	return &assignAndResult{
		assign: assign,
		result: nil,
	}
}

type TaskAssignSet struct {
	set *list.List
}

func NewTaskAssignSet() *TaskAssignSet {
	return &TaskAssignSet{
		set: list.New(),
	}
}

func (t *TaskAssignSet) Add(taskAssign *TaskAssign) {
	t.set.PushBack(newAssignAndResult(taskAssign))
}

func (t *TaskAssignSet) AddResult(result *TaskPartialResult) {
	for e := t.set.Front(); e != nil; e = e.Next() {
		if p := e.Value.(*assignAndResult); p.assign.ID == result.ID {
			p.result = result
			break
		}
	}
}

func (t *TaskAssignSet) Remove(taskAssign *TaskAssign) {
	for e := t.set.Front(); e != nil; e = e.Next() {
		if e.Value.(*assignAndResult).assign.Equals(taskAssign) {
			t.set.Remove(e)
		}
	}
}

func (t *TaskAssignSet) Contains(taskAssign *TaskAssign) bool {
	for e := t.set.Front(); e != nil; e = e.Next() {
		if e.Value.(*assignAndResult).assign.Equals(taskAssign) {
			return true
		}
	}
	return false
}

func (t *TaskAssignSet) GetAllAssign() []*TaskAssign {
	assigns := make([]*TaskAssign, 0)
	for e := t.set.Front(); e != nil; e = e.Next() {
		assigns = append(assigns, e.Value.(*assignAndResult).assign)
	}
	return assigns
}

// return all non-nil results
func (t *TaskAssignSet) GetAllResult() []*TaskPartialResult {
	results := make([]*TaskPartialResult, 0)
	for e := t.set.Front(); e != nil; e = e.Next() {
		result := e.Value.(*assignAndResult).result
		if result != nil {
			results = append(results, result)
		}
	}
	return results
}

func (t *TaskAssignSet) RemoveTaskAssignByID(id WorkerID) {
	for e := t.set.Front(); e != nil; e = e.Next() {
		workerID := e.Value.(*assignAndResult).assign.ID
		if workerID == id {
			t.set.Remove(e)
		}
	}
}
