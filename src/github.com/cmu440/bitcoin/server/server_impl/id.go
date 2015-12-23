package server

import (
	"fmt"
	"github.com/cmu440/bitcoin"
)

type IDManager struct {
	nextRequestID   int
	requestToClient map[int]int
	clientIDs       *bitcoin.IntSet
	workerIDs       *bitcoin.IntSet
}

func NewIDManager() *IDManager {
	return &IDManager{
		nextRequestID:   0,
		requestToClient: make(map[int]int),
		clientIDs:       bitcoin.NewIntSet(),
		workerIDs:       bitcoin.NewIntSet(),
	}
}

func (m *IDManager) GetUniqueRequestID() int {
	id := m.nextRequestID
	m.nextRequestID++
	return id
}

func (m *IDManager) AddWorkerIfAbsent(workerID int) {
	m.workerIDs.Add(workerID)
}

func (m *IDManager) AddClientIfAnsent(clientID int) {
	m.clientIDs.Add(clientID)
}

func (m *IDManager) IsWorker(id int) bool {
	return m.workerIDs.Contains(id)
}

func (m *IDManager) IsClient(id int) bool {
	return m.clientIDs.Contains(id)
}

func (m *IDManager) RemoveWorker(workerID int) {
	m.workerIDs.Remove(workerID)
}

func (m *IDManager) RemoveClient(clientID int) {
	m.clientIDs.Remove(clientID)
}

func (m *IDManager) RemoveID(id int) {
	if m.IsClient(id) {
		m.RemoveClient(id)
	} else if m.IsWorker(id) {
		m.RemoveWorker(id)
	}
}

func (m *IDManager) SetRequestToClient(requestID, clientID int) {
	m.requestToClient[requestID] = clientID
}

func (m *IDManager) GetClientByRequest(requestID int) (int, error) {
	clientID, exists := m.requestToClient[requestID]
	if !exists {
		return -1, fmt.Errorf("client ID does not exists")
	} else {
		return clientID, nil
	}
}
