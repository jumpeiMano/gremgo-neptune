package gremgo

import (
	"context"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"
)

const connRequestQueueSize = 1000000

// errors
var (
	ErrGraphDBClosed = errors.New("graphdb is closed")
	ErrBadConn       = errors.New("bad conn")
)

// Pool maintains a list of connections.
type Pool struct {
	MaxOpen      int
	MaxLifetime  time.Duration
	dial         func() (*Client, error)
	mu           sync.Mutex
	freeConns    []*PooledConnection
	open         int
	openerCh     chan struct{}
	connRequests map[uint64]chan connRequest
	nextRequest  uint64
	cleanerCh    chan struct{}
	closed       bool
}

// NewPool create ConnectionPool
func NewPool(dial func() (*Client, error)) *Pool {
	p := new(Pool)
	p.dial = dial
	p.openerCh = make(chan struct{}, connRequestQueueSize)
	p.connRequests = make(map[uint64]chan connRequest)

	go p.opener()

	return p
}

type connRequest struct {
	pc  *PooledConnection
	err error
}

// PooledConnection represents a shared and reusable connection.
type PooledConnection struct {
	Pool   *Pool
	Client *Client
	t      time.Time
}

func (p *Pool) maybeOpenNewConnections() {
	if p.closed {
		return
	}
	numRequests := len(p.connRequests)
	if p.MaxOpen > 0 {
		numCanOpen := p.MaxOpen - p.open
		if numRequests > numCanOpen {
			numRequests = numCanOpen
		}
	}
	for numRequests > 0 {
		p.open++
		numRequests--
		p.openerCh <- struct{}{}
	}
}

func (p *Pool) opener() {
	for range p.openerCh {
		p.openNewConnection()
	}
}

func (p *Pool) openNewConnection() {
	if p.closed {
		p.mu.Lock()
		p.open--
		p.mu.Unlock()
		return
	}
	c, err := p.dial()
	if err != nil {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.open--
		p.maybeOpenNewConnections()
		return
	}
	pc := &PooledConnection{
		Pool:   p,
		Client: c,
		t:      time.Now(),
	}
	p.mu.Lock()
	if !p.putConnLocked(pc, nil) {
		p.open--
		p.mu.Unlock()
		pc.Client.Close()
		return
	}
	p.mu.Unlock()
	return
}

// PutConn is return connetion to the connection pool.
func (p *Pool) PutConn(pc *PooledConnection, err error) error {
	p.mu.Lock()
	if !p.putConnLocked(pc, err) {
		p.open--
		p.mu.Unlock()
		pc.Client.Close()
		return err
	}
	p.mu.Unlock()
	return err
}

func (p *Pool) putConnLocked(pc *PooledConnection, err error) bool {
	if p.closed {
		return false
	}
	if p.MaxOpen > 0 && p.MaxOpen < p.open {
		return false
	}
	if len(p.connRequests) > 0 {
		var req chan connRequest
		var reqKey uint64
		for reqKey, req = range p.connRequests {
			break
		}
		delete(p.connRequests, reqKey)
		req <- connRequest{
			pc:  pc,
			err: err,
		}
	} else {
		p.freeConns = append(p.freeConns, pc)
		p.startCleanerLocked()
	}
	return true
}

// get will return an available pooled connection. Either an idle connection or
// by dialing a new one if the pool does not currently have a maximum number
// of active connections.
func (p *Pool) get() (*PooledConnection, error) {
	ctx := context.Background()
	cn, err := p.conn(ctx, true)
	if err == nil {
		return cn, nil
	}
	if errors.Cause(err) == ErrBadConn {
		return p.conn(ctx, false)
	}
	return cn, err
}

func (p *Pool) conn(ctx context.Context, useFreeConn bool) (*PooledConnection, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrGraphDBClosed
	}
	// Check if the context is expired.
	select {
	default:
	case <-ctx.Done():
		p.mu.Unlock()
		return nil, errors.Wrap(ctx.Err(), "the context is expired")
	}
	lifetime := p.MaxLifetime

	var pc *PooledConnection
	numFree := len(p.freeConns)
	if useFreeConn && numFree > 0 {
		pc = p.freeConns[0]
		copy(p.freeConns, p.freeConns[1:])
		p.freeConns = p.freeConns[:numFree-1]
		p.mu.Unlock()
		if pc.expired(lifetime) {
			p.mu.Lock()
			p.open--
			p.mu.Unlock()
			pc.Client.Close()
			return nil, ErrBadConn
		}
		return pc, nil
	}

	if p.MaxOpen > 0 && p.MaxOpen <= p.open {
		req := make(chan connRequest, 1)
		reqKey := p.nextRequest
		p.nextRequest++
		p.connRequests[reqKey] = req
		p.mu.Unlock()

		select {
		// timeout
		case <-ctx.Done():
			// Remove the connection request and ensure no value has been sent
			// on it after removing.
			p.mu.Lock()
			delete(p.connRequests, reqKey)
			p.mu.Unlock()
			select {
			case ret, ok := <-req:
				if ok {
					p.PutConn(ret.pc, ret.err)
				}
			default:
			}
			return nil, errors.Wrap(ctx.Err(), "Deadline of connRequests exceeded")
		case ret, ok := <-req:
			if !ok {
				return nil, ErrGraphDBClosed
			}
			if ret.err != nil {
				return ret.pc, errors.Wrap(ret.err, "Response has an error")
			}
			return ret.pc, nil
		}
	}

	p.open++
	p.mu.Unlock()
	newCn, err := p.dial()
	if err != nil {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.open--
		p.maybeOpenNewConnections()
		return nil, errors.Wrap(err, "Failed newConn")
	}
	return &PooledConnection{
		Pool:   p,
		Client: newCn,
		t:      time.Now(),
	}, nil
}

func (p *Pool) needStartCleaner() bool {
	return p.MaxLifetime > 0 &&
		p.open > 0 &&
		p.cleanerCh == nil
}

// startCleanerLocked starts connectionCleaner if needed.
func (p *Pool) startCleanerLocked() {
	if p.needStartCleaner() {
		p.cleanerCh = make(chan struct{}, 1)
		go p.connectionCleaner()
	}
}

func (p *Pool) connectionCleaner() {
	const minInterval = time.Second

	d := p.MaxLifetime
	if d < minInterval {
		d = minInterval
	}
	t := time.NewTimer(d)

	for {
		select {
		case <-t.C:
		case <-p.cleanerCh: // dbclient was closed.
		}

		ml := p.MaxLifetime
		p.mu.Lock()
		if p.closed || len(p.freeConns) == 0 || ml <= 0 {
			p.cleanerCh = nil
			p.mu.Unlock()
			return
		}
		n := time.Now()
		mlExpiredSince := n.Add(-ml)
		var closing []*PooledConnection
		for i := 0; i < len(p.freeConns); i++ {
			pc := p.freeConns[i]
			if (ml > 0 && pc.t.Before(mlExpiredSince)) ||
				pc.Client.Errored {
				p.open--
				closing = append(closing, pc)
				last := len(p.freeConns) - 1
				p.freeConns[i] = p.freeConns[last]
				p.freeConns[last] = nil
				p.freeConns = p.freeConns[:last]
				i--
			}
		}
		p.mu.Unlock()

		for _, pc := range closing {
			if pc.Client != nil {
				pc.Client.Close()
			}
		}

		t.Reset(d)
	}
}

// ExecuteWithBindings formats a raw Gremlin query, sends it to Gremlin Server, and returns the result.
func (p *Pool) ExecuteWithBindings(query string, bindings, rebindings map[string]string) (resp []Response, err error) {
	pc, err := p.get()
	if err != nil {
		return resp, errors.Wrap(err, "Failed p.Get")
	}
	defer func() {
		p.PutConn(pc, err)
	}()
	resp, err = pc.Client.executeRequest(query, &bindings, &rebindings)
	return
}

// Execute formats a raw Gremlin query, sends it to Gremlin Server, and returns the result.
func (p *Pool) Execute(query string) (resp []Response, err error) {
	pc, err := p.get()
	if err != nil {
		return resp, errors.Wrap(err, "Failed p.Get")
	}
	defer func() {
		p.PutConn(pc, err)
	}()
	resp, err = pc.Client.executeRequest(query, nil, nil)
	return
}

// ExecuteFile takes a file path to a Gremlin script, sends it to Gremlin Server, and returns the result.
func (p *Pool) ExecuteFile(path string, bindings, rebindings map[string]string) (resp []Response, err error) {
	pc, err := p.get()
	if err != nil {
		return resp, errors.Wrap(err, "Failed p.Get")
	}
	defer func() {
		p.PutConn(pc, err)
	}()
	d, err := ioutil.ReadFile(path) // Read script from file
	if err != nil {
		log.Println(err)
		return
	}
	query := string(d)
	resp, err = pc.Client.executeRequest(query, &bindings, &rebindings)
	return
}

// Close closes the pool.
func (p *Pool) Close() {
	p.mu.Lock()

	close(p.openerCh)
	if p.cleanerCh != nil {
		close(p.cleanerCh)
	}
	for _, cr := range p.connRequests {
		close(cr)
	}
	p.closed = true
	p.mu.Unlock()
	for _, pc := range p.freeConns {
		if pc.Client != nil {
			pc.Client.Close()
		}
	}
	p.mu.Lock()
	p.freeConns = nil
	p.mu.Unlock()
}

func (pc *PooledConnection) expired(timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	return pc.t.Add(timeout).Before(time.Now())
}
