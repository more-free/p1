package server

// TODO add stat and runtime to maintain worker metadata, stat, runtime info (dynamic), etc.
// these parameters can be used as input of the scheduler. Accordingly, scheduler should be
// stateless

import (
	"container/list"
	"fmt"
	"github.com/cmu440/bitcoin"
)

type RequestID int
type WorkerID int

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
		taskToStart:  taskToStart,
		taskResult:   taskResult,
		pendingTask:  NewTaskQueue(),
	}
}

/*
 non-blocking
*/
func (w *OneForOneWorkerProxy) Schedule(newTask *Task) {
	if w.getWorkerSize() == 0 {
		w.pendingTask.Add(newTask)
		return
	}

	assignments := w.mapper.GetAssignment(newTask)

	// update request table
	for _, assign := range assignments {
		w.requestTable.AddTaskAssign(assign.Task.ID, assign)
	}

	// update task table
	for _, assign := range assignments {
		w.taskTable.AddTask(assign.ID, assign.Task)

		// if no other running tasks, notify the taskToStart channel
		if w.taskTable.GetTaskQueue(assign.ID).Len() == 1 {
			w.taskToStart <- assign
			w.mapper.SetTaskStarted(assign.ID, assign.Task)
		}
	}
}

func (w *OneForOneWorkerProxy) getWorkerSize() int {
	return len(w.taskTable.GetWorkers())
}

func (w *OneForOneWorkerProxy) HandlePartialResult(partialResult *TaskPartialResult) {
	// get task instance of give partialResult from task table
	workerID := partialResult.ID
	finishedTask, _ := w.taskTable.PopTask(workerID)
	requestID := finishedTask.ID

	// update stateful scheduler
	w.mapper.SetTaskFinished(workerID, finishedTask)

	// set result for request table
	w.requestTable.AddTaskResult(requestID, partialResult)

	// send message to taskResult channel if all sub-tasks of the request have been finished
	if len(w.requestTable.GetTaskAssign(requestID)) == len(w.requestTable.GetTaskResult(requestID)) {
		w.requestTable.RemoveRequest(requestID)
		w.taskResult <- w.reducer(w.requestTable.GetTaskResult(requestID), requestID)
	}

	// update task table by starting the next task of workerID
	newTask, err := w.taskTable.GetTopTask(workerID)
	if err == nil { // if it has task left, start it
		w.taskToStart <- NewTaskAssign(workerID, newTask)
	}
}

// it re-assigns all tasks belonging to the failed worker to other workers
func (w *OneForOneWorkerProxy) HandleWorkerLost(id WorkerID) {
	w.mapper.RemoveWorker(id)

	failedTasks := w.taskTable.GetTaskQueue(id).GetAll()
	w.taskTable.RemoveWorker(id)

	// remove failed tasks from request table
	for _, failedTask := range failedTasks {
		w.requestTable.RemoveTaskAssign(failedTask.ID, NewTaskAssign(id, failedTask))
	}

	// reschedule failed tasks
	for _, failedTask := range failedTasks {
		w.Schedule(failedTask)
	}
}

func (w *OneForOneWorkerProxy) HandleWorkerJoin(id WorkerID) {
	w.mapper.AddWorker(id)
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
