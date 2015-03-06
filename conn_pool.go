package pool

import (
	"bytes"
	"errors"
	"fmt"
	logger "github.com/nporsche/golang-logger"
	"sync/atomic"
	"time"
)

type IConnection interface {
	Close()
	HostAddr() string
}

////////////////////////////////////
const (
	retryDuration    = time.Second * 10
	healthReportTick = time.Second * 30
)

const (
	statConnected     = 1
	stateConnecting   = 2
	stateDisconnected = 3
)

var svrDownError = errors.New("backend servers are all down")
var healthStateDef map[int]string

type CreateConnectionFunc func(host string) (IConnection, error)

type health struct {
	state int
	idle  int32
	total int32
}

type ConnPool struct {
	connections      map[string]chan IConnection
	healthState      map[string]*health
	createConnection CreateConnectionFunc
	//for robin
	hosts   []string
	hosts_i uint32
}

func init() {
	healthStateDef = make(map[int]string, 0)
	healthStateDef[statConnected] = "Connected"
	healthStateDef[stateConnecting] = "Connecting"
	healthStateDef[stateDisconnected] = "Disconnected"
}

func NewConnPool(maxIdle uint, backwardHosts []string, createFunc CreateConnectionFunc) *ConnPool {
	this := new(ConnPool)
	this.connections = make(map[string]chan IConnection)
	this.healthState = make(map[string]*health)
	for _, host := range backwardHosts {
		this.connections[host] = make(chan IConnection, maxIdle)
		this.healthState[host] = &health{stateConnecting, 0, 0}
	}
	this.hosts = backwardHosts
	this.hosts_i = 0
	this.createConnection = createFunc
	go this.healthReport()
	return this
}

//thread dangerous
func (this *ConnPool) GetByHost(host string) (conn IConnection, err error) {
	select {
	case conn = <-this.connections[host]:
		atomic.AddInt32(&this.healthState[host].idle, -1)
	default:
		conn, err = this.createConnection(host)
		if err == nil {
			atomic.AddInt32(&this.healthState[host].total, 1)
			this.healthState[host].state = statConnected
		} else {
			this.turnOff(host, err)
		}
	}
	return
}

func (this *ConnPool) Get() (conn IConnection, err error) {
	for i := 0; i < len(this.hosts); i++ {
		var host string
		host, err = this.getRobinHost()
		if err == nil {
			conn, err = this.GetByHost(host)
			if err == nil {
				return
			}
		}
	}
	return nil, svrDownError
}

//thread dangerous
func (this *ConnPool) Return(conn IConnection) {
	if conn != nil {
		//return back to pool
		select {
		case this.connections[conn.HostAddr()] <- conn:
			atomic.AddInt32(&this.healthState[conn.HostAddr()].idle, 1)
		default:
			atomic.AddInt32(&this.healthState[conn.HostAddr()].total, -1)
			logger.Infof("Connection host=[%s] released due to pool is full", conn.HostAddr())
			conn.Close()
		}
	}
}

func (this *ConnPool) TurnOff(conn IConnection, err error) {
	if conn != nil {
		conn.Close()
		this.turnOff(conn.HostAddr(), err)
	}
}

func (this *ConnPool) turnOff(host string, err error) {
	logger.Errorf("turn off backward host=[%s] due to [%v]", host, err)
	//clean state and idle connections of this addr
	this.healthState[host].state = stateDisconnected
	this.healthState[host].idle = 0
	this.healthState[host].total = 0
	this.cleanIdle(host)
	time.AfterFunc(
		retryDuration,
		func() {
			this.healthState[host].state = stateConnecting
		})
}

func (this *ConnPool) getRobinHost() (string, error) {
	for i := 0; i < len(this.hosts); i++ {
		atomic.AddUint32(&this.hosts_i, 1)
		host := this.hosts[this.hosts_i%uint32(len(this.hosts))]
		if this.healthState[host].state != stateDisconnected {
			return host, nil
		}
	}
	return "", svrDownError
}

func (this *ConnPool) healthReport() {
	ch := time.Tick(healthReportTick)
	for {
		<-ch
		var buf bytes.Buffer
		for k, v := range this.healthState {
			buf.WriteString(fmt.Sprintf("host=[%s] state=[%s] idle=[%d] total=[%d]|", k, healthStateDef[v.state], v.idle, v.total))
		}
		logger.Info(buf.String())
	}
}

func (this *ConnPool) cleanIdle(host string) {
	for {
		select {
		case conn := <-this.connections[host]:
			conn.Close()
		default:
			return
		}
	}
}
