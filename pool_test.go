package gremgo

import (
	"fmt"
	"testing"
	"time"
)

var dummyDialFunc func() (*Client, error)

func TestConnectionCleaner(t *testing.T) {
	n := time.Now()

	// invalid has timedout and should be cleaned up
	invalid := &PooledConnection{Client: &Client{}, t: n.Add(-1030 * time.Millisecond)}
	// valid has not yet timed out and should remain in the freeConns pool
	valid := &PooledConnection{Client: &Client{}, t: n.Add(1030 * time.Millisecond)}

	// Pool has a 30 second timeout and an freeConns connection slice containing both
	// the invalid and valid freeConns connections
	p := NewPool(dummyDialFunc)
	defer p.Close()
	p.MaxLifetime = time.Second * 1
	p.freeConns = []*PooledConnection{invalid, valid}

	if len(p.freeConns) != 2 {
		t.Errorf("Expected 2 freeConns connections, got %d", len(p.freeConns))
	}

	p.mu.Lock()
	p.open = len(p.freeConns)
	p.startCleanerLocked()
	p.mu.Unlock()

	time.Sleep(1010 * time.Millisecond)
	if len(p.freeConns) != 1 {
		t.Errorf("Expected 1 freeConns connection after clean, got %d", len(p.freeConns))
		for _, pc := range p.freeConns {
			fmt.Println(pc.t)
		}
	}

	if p.freeConns[0].t != valid.t {
		t.Error("Expected the valid connection to remain in freeConns pool")
	}

}

func TestPurgeErrorClosedConnection(t *testing.T) {
	n := time.Now()

	p := NewPool(dummyDialFunc)
	defer p.Close()
	p.MaxLifetime = time.Second * 1

	valid := &PooledConnection{Client: &Client{}, t: n.Add(1030 * time.Millisecond)}

	client := &Client{}

	closed := &PooledConnection{Pool: p, Client: client, t: n.Add(1030 * time.Millisecond)}

	freeConns := []*PooledConnection{valid, closed}

	p.freeConns = freeConns

	// Simulate error
	closed.Client.Errored = true

	if len(p.freeConns) != 2 {
		t.Errorf("Expected 2 idle connections, got %d", len(p.freeConns))
	}

	p.mu.Lock()
	p.open = len(p.freeConns)
	p.startCleanerLocked()
	p.mu.Unlock()
	time.Sleep(1010 * time.Millisecond)

	if len(p.freeConns) != 1 {
		t.Errorf("Expected 1 freeConns connection after clean, got %d", len(p.freeConns))
	}

	if p.freeConns[0] != valid {
		t.Error("Expected valid connection to remain in pool")
	}
}

func TestPooledConnectionClose(t *testing.T) {
	pool := NewPool(dummyDialFunc)
	defer pool.Close()
	pc := &PooledConnection{Pool: pool}

	if len(pool.freeConns) != 0 {
		t.Errorf("Expected 0 freeConns connection, got %d", len(pool.freeConns))
	}

	pool.PutConn(pc, nil)

	if len(pool.freeConns) != 1 {
		t.Errorf("Expected 1 freeConns connection, got %d", len(pool.freeConns))
	}

	freeConnsd := pool.freeConns[0]

	if freeConnsd == nil {
		t.Error("Expected to get connection")
	}
}

func TestFirst(t *testing.T) {
	n := time.Now()
	pool := NewPool(dummyDialFunc)
	defer pool.Close()
	pool.MaxOpen = 1
	pool.MaxLifetime = 30 * time.Millisecond
	freeConnsd := []*PooledConnection{
		&PooledConnection{Pool: pool, Client: &Client{}, t: n.Add(-45 * time.Millisecond)}, // expired
		&PooledConnection{Pool: pool, Client: &Client{}, t: n.Add(-45 * time.Millisecond)}, // expired
		&PooledConnection{Pool: pool, Client: &Client{}},                                   // valid
	}
	pool.freeConns = freeConnsd

	if len(pool.freeConns) != 3 {
		t.Errorf("Expected 3 freeConns connection, got %d", len(pool.freeConns))
	}

	// Empty pool should return nil
	emptyPool := &Pool{}

	if len(emptyPool.freeConns) != 0 {
		t.Errorf("Expected nil, got %d", len(emptyPool.freeConns))
	}
}

func TestGetAndDial(t *testing.T) {
	n := time.Now()

	client := &Client{}
	pool := NewPool(func() (*Client, error) {
		return client, nil
	})
	defer pool.Close()
	pool.MaxLifetime = time.Millisecond * 30

	invalid := &PooledConnection{Pool: pool, Client: &Client{}, t: n.Add(-30 * time.Millisecond)}

	freeConns := []*PooledConnection{invalid}
	pool.freeConns = freeConns

	if len(pool.freeConns) != 1 {
		t.Error("Expected 1 freeConns connection")
	}

	if pool.freeConns[0] != invalid {
		t.Error("Expected invalid connection")
	}

	pool.mu.Lock()
	pool.startCleanerLocked()
	pool.mu.Unlock()
	time.Sleep(1010 * time.Millisecond)

	conn, err := pool.get()

	if err != nil {
		t.Error(err)
	}

	if len(pool.freeConns) != 0 {
		t.Errorf("Expected 0 freeConns connections, got %d", len(pool.freeConns))
	}

	if conn.Client != client {
		t.Error("Expected correct client to be returned")
	}

	if pool.open != 0 {
		t.Errorf("Expected 0 opened connection, got %d", pool.open)
	}

	// Close the connection and ensure it was returned to the freeConns pool
	pool.PutConn(conn, nil)

	if len(pool.freeConns) != 1 {
		t.Error("Expected connection to be returned to freeConns pool")
	}

	// Get a new connection and ensure that it is the now idling connection
	conn, err = pool.get()

	if err != nil {
		t.Error(err)
	}

	if conn.Client != client {
		t.Error("Expected the same connection to be reused")
	}
}
