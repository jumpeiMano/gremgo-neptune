package gremgo

import (
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// Client is a container for the gremgo client.
type Client struct {
	conn             dialer
	requests         chan []byte
	responses        chan []byte
	results          *sync.Map
	responseNotifier *sync.Map // responseNotifier notifies the requester that a response has arrived for the request
	mu               sync.RWMutex
	Errored          bool
}

// NewDialer returns a WebSocket dialer to use when connecting to Gremlin Server
func NewDialer(host string, configs ...DialerConfig) (dialer *Ws) {
	dialer = &Ws{
		timeout:      5 * time.Second,
		pingInterval: 60 * time.Second,
		writingWait:  15 * time.Second,
		readingWait:  15 * time.Second,
		connected:    false,
		quit:         make(chan struct{}),
	}

	for _, conf := range configs {
		conf(dialer)
	}

	dialer.host = host
	return dialer
}

func newClient() (c Client) {
	c.requests = make(chan []byte, 3)  // c.requests takes any request and delivers it to the WriteWorker for dispatch to Gremlin Server
	c.responses = make(chan []byte, 3) // c.responses takes raw responses from ReadWorker and delivers it for sorting to handelResponse
	c.results = &sync.Map{}
	c.responseNotifier = &sync.Map{}
	return
}

// Dial returns a gremgo client for interaction with the Gremlin Server specified in the host IP.
func Dial(conn dialer, errs chan error) (c Client, err error) {
	c = newClient()
	c.conn = conn

	// Connects to Gremlin Server
	err = conn.connect()
	if err != nil {
		return
	}

	quit := conn.(*Ws).quit

	go c.writeWorker(errs, quit)
	go c.readWorker(errs, quit)
	go conn.ping(errs)

	return
}

func (c *Client) executeRequest(query string, bindings, rebindings *map[string]string) (resp []Response, err error) {
	if c.conn.isDisposed() {
		return resp, errors.New("you cannot write on disposed connection")
	}
	var req request
	var id string
	if bindings != nil && rebindings != nil {
		req, id, err = prepareRequestWithBindings(query, *bindings, *rebindings)
	} else {
		req, id, err = prepareRequest(query)
	}
	if err != nil {
		return
	}

	msg, err := packageRequest(req)
	if err != nil {
		log.Println(err)
		return
	}
	c.responseNotifier.Store(id, make(chan error, 1))
	c.dispatchRequest(msg)
	resp, err = c.retrieveResponse(id)
	if err != nil {
		err = errors.Wrapf(err, "query: %s", query)
	}
	return
}

func (c *Client) authenticate(requestID string) (err error) {
	auth := c.conn.getAuth()
	req, err := prepareAuthRequest(requestID, auth.username, auth.password)
	if err != nil {
		return
	}

	msg, err := packageRequest(req)
	if err != nil {
		log.Println(err)
		return
	}

	c.dispatchRequest(msg)
	return
}

// Close closes the underlying connection and marks the client as closed.
func (c *Client) Close() {
	if c.conn != nil {
		c.conn.close()
	}
}
