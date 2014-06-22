package ui

import (
	"sync"

	"github.com/meeko/meekod/broker/services/rpc"
)

type rpcMiddleware struct {
	rpc.Exchange
	agents map[string]map[string]struct{}
	lock   *sync.Mutex
}

func newRpcMiddleware(exchange rpc.Exchange) *rpcMiddleware {
	return &rpcMiddleware{
		Exchange: exchange,
		agents:   make(map[string]map[string]struct{}),
		lock:     new(sync.Mutex),
	}
}

func newRpcAgentRecord() map[string]struct{} {
	return make(map[string]struct{})
}

// Overwrites rpc.Exchange -----------------------------------------------------

func (m *rpcMiddleware) RegisterMethod(agent string, endpoint rpc.Endpoint, method string) error {
	m.lock.Lock()
	record, ok := m.agents[agent]
	if !ok {
		record = newRpcAgentRecord()
		m.agents[agent] = record
	}
	record[method] = struct{}{}
	m.lock.Unlock()
	return m.Exchange.RegisterMethod(agent, endpoint, method)
}

func (m *rpcMiddleware) UnregisterMethod(agent string, method string) {
	m.lock.Lock()
	record, ok := m.agents[agent]
	if ok {
		delete(record, method)
	}
	m.lock.Unlock()
	m.Exchange.UnregisterMethod(agent, method)
}

func (m *rpcMiddleware) UnregisterApp(agent string) {
	m.lock.Lock()
	delete(m.agents, agent)
	m.lock.Unlock()
}
