package pool

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
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
var healthStateDef map[int]string = map[int]string{StateConnected: "Connected",
	StateConnecting:   "Connecting",
	StateDisconnected: "Disconnected"}

type Connectionfactory func(host string) (IConnection, error)

type Health struct {
	State int
	Idle  int32
	Total int32
}

type ConnectionEntry struct {
	health   *Health
	connList chan IConnection
}

type ConnPool struct {
	connMap map[string]*ConnectionEntry
	factory Connectionfactory
	//for robin
	hosts   []string
	hosts_i uint32
	mtx     sync.Mutex
	//capacity
	maxIdle int
}

func NewConnPool(maxIdle int, factory Connectionfactory) *ConnPool {
	this := new(ConnPool)
	this.connMap = make(map[string]*ConnectionEntry, 0)
	this.factory = factory
	this.maxIdle = maxIdle
	this.hosts_i = 0

	return this
}

func (this *ConnPool) Hosts() []string {
	return this.hosts
}

func (this *ConnPool) Health(host string) *Health {
	return this.connMap[host].health
}

//thread dangerous
func (this *ConnPool) GetByHost(host string) (conn IConnection, err error) {
	if !this.checkHostExist(host) {
		this.addHostEntry(host)
	}
	if this.Health(host).State == StateDisconnected {
		return nil, svrDownError
	} else {
		select {
		case conn = <-this.connMap[host].connList:
			atomic.AddInt32(&this.Health(host).Idle, -1)
		default:
			conn, err = this.factory(host)
			if err == nil {
				atomic.AddInt32(&this.Health(host).Total, 1)
				this.Health(host).State = StateConnected
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
		if this.Health(conn.HostAddr()).State == StateConnected {
			//return back to pool
			select {
			case this.connMap[conn.HostAddr()].connList <- conn:
				atomic.AddInt32(&this.Health(conn.HostAddr()).Idle, 1)
			default:
				atomic.AddInt32(&this.Health(conn.HostAddr()).Total, -1)
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
	if this.Health(host).State == StateConnected {
		//clean state and idle connections of this addr
		this.Health(host).State = StateDisconnected
		this.Health(host).Idle = 0
		this.Health(host).Total = 0
		this.cleanIdle(host)
		time.AfterFunc(
			retryDuration,
			func() {
				this.Health(host).State = StateConnecting
			})
	}
}

func (this *ConnPool) getRobinHost() string {
	atomic.AddUint32(&this.hosts_i, 1)
	return this.hosts[this.hosts_i%uint32(len(this.hosts))]
}

func (this *ConnPool) HealthMessage() string {
	var buf bytes.Buffer
	for k, v := range this.connMap {
		buf.WriteString(fmt.Sprintf("host=[%s] state=[%s] idle=[%d] total=[%d]|", k, healthStateDef[v.health.State], v.health.Idle, v.health.Total))
	}
	return buf.String()
}

func (this *ConnPool) cleanIdle(host string) {
	for {
		select {
		case conn := <-this.connMap[host].connList:
			conn.Close()
		default:
			return
		}
	}
}

func (this *ConnPool) addHostEntry(host string) {
	this.mtx.Lock()
	if !this.checkHostExist(host) {
		connList := make(chan IConnection, this.maxIdle)
		this.connMap[host] = &ConnectionEntry{&Health{StateConnecting, 0, 0}, connList}
		this.hosts = append(this.hosts, host)
	}
	this.mtx.Unlock()
}

func (this *ConnPool) checkHostExist(host string) bool {
	_, ok := this.connMap[host]
	return ok
}
