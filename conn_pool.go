package pool

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/nporsche/np-golang-logger"
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
	StateConnected    = 1
	StateConnecting   = 2
	StateDisconnected = 3
)

var svrDownError = errors.New("backend servers are all down")
var healthStateDef map[int]string

type CreateConnectionFunc func(host string) (IConnection, error)

type Health struct {
	State int
	Idle  int32
	Total int32
}

type ConnPool struct {
	connections      map[string]chan IConnection
	healthState      map[string]*Health
	createConnection CreateConnectionFunc
	//for robin
	hosts   []string
	hosts_i uint32
}

func init() {
	healthStateDef = make(map[int]string, 0)
	healthStateDef[StateConnected] = "Connected"
	healthStateDef[StateConnecting] = "Connecting"
	healthStateDef[StateDisconnected] = "Disconnected"
}

func NewConnPool(maxIdle uint, backwardHosts []string, createFunc CreateConnectionFunc) *ConnPool {
	this := new(ConnPool)
	this.connections = make(map[string]chan IConnection)
	this.healthState = make(map[string]*Health)
	for _, host := range backwardHosts {
		this.connections[host] = make(chan IConnection, maxIdle)
		this.healthState[host] = &Health{StateConnecting, 0, 0}
	}
	this.hosts = backwardHosts
	this.hosts_i = 0
	this.createConnection = createFunc
	go this.healthReport()
	return this
}

func (this *ConnPool) Hosts() []string {
	return this.hosts
}

func (this *ConnPool) Health(host string) *Health {
	return this.healthState[host]
}

//thread dangerous
func (this *ConnPool) GetByHost(host string) (conn IConnection, err error) {
	if this.healthState[host].State == StateDisconnected {
		return nil, svrDownError
	} else {
		select {
		case conn = <-this.connections[host]:
			atomic.AddInt32(&this.healthState[host].Idle, -1)
		default:
			conn, err = this.createConnection(host)
			if err == nil {
				atomic.AddInt32(&this.healthState[host].Total, 1)
				this.healthState[host].State = StateConnected
			} else {
				this.turnOff(host, err)
			}
		}
	}
	return
}

func (this *ConnPool) Get() (conn IConnection, err error) {
	for i := 0; i < len(this.hosts); i++ {
		host := this.getRobinHost()
		conn, err = this.GetByHost(host)
		if err == nil {
			break
		}
	}
	return
}

//thread dangerous
func (this *ConnPool) Return(conn IConnection) {
	if conn != nil {
		if this.healthState[conn.HostAddr()].State == StateConnected {
			//return back to pool
			select {
			case this.connections[conn.HostAddr()] <- conn:
				atomic.AddInt32(&this.healthState[conn.HostAddr()].Idle, 1)
			default:
				atomic.AddInt32(&this.healthState[conn.HostAddr()].Total, -1)
				logger.Infof("Connection host=[%s] released due to pool is full", conn.HostAddr())
				conn.Close()
			}
		} else {
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
	if this.healthState[host].State == StateConnected {
		logger.Errorf("turn off backward host=[%s] due to [%v]", host, err)
		//clean state and idle connections of this addr
		this.healthState[host].State = StateDisconnected
		this.healthState[host].Idle = 0
		this.healthState[host].Total = 0
		this.cleanIdle(host)
		time.AfterFunc(
			retryDuration,
			func() {
				this.healthState[host].State = StateConnecting
			})
	}
}

func (this *ConnPool) getRobinHost() string {
	atomic.AddUint32(&this.hosts_i, 1)
	return this.hosts[this.hosts_i%uint32(len(this.hosts))]
}

func (this *ConnPool) healthReport() {
	ch := time.Tick(healthReportTick)
	for {
		<-ch
		var buf bytes.Buffer
		for k, v := range this.healthState {
			buf.WriteString(fmt.Sprintf("host=[%s] state=[%s] idle=[%d] total=[%d]|", k, healthStateDef[v.State], v.Idle, v.Total))
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
