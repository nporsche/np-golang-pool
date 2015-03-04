package pool

import "bytes"

type BufferPool struct {
	buffers chan *bytes.Buffer
}

func NewBufferPool(maxIdleBuffers int) *BufferPool {
	bufs := make(chan *bytes.Buffer, maxIdleBuffers)
	return &BufferPool{bufs}
}

func (this *BufferPool) Get() *bytes.Buffer {
	select {
	case buf := <-this.buffers:
		return buf
	default:
		return new(bytes.Buffer)
	}
}

func (this *BufferPool) Release(buf *bytes.Buffer) {
	buf.Reset()
	select {
	case this.buffers <- buf:
	default:
	}
}
