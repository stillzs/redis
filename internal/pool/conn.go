package pool

import (
	"bufio"
	"context"
	"net"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8/internal"
	"github.com/go-redis/redis/v8/internal/proto"
)

var noDeadline = time.Time{}

type Conn struct {
	usedAt  int64    //使用时间atomic
	netConn net.Conn //tcp的网络连接

	rd *proto.Reader //redis协议实现的读取
	bw *bufio.Writer
	wr *proto.Writer //redis协议实现的写入

	Inited    bool      //是否完成初始化
	pooled    bool      //是否放入连接池
	createdAt time.Time //创建时间
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		netConn:   netConn,
		createdAt: time.Now(),
	}
	cn.rd = proto.NewReader(netConn)
	cn.bw = bufio.NewWriter(netConn)
	cn.wr = proto.NewWriter(cn.bw)
	cn.SetUsedAt(time.Now())
	return cn
}

func (cn *Conn) UsedAt() time.Time {
	unix := atomic.LoadInt64(&cn.usedAt)
	return time.Unix(unix, 0)
}

func (cn *Conn) SetUsedAt(tm time.Time) {
	atomic.StoreInt64(&cn.usedAt, tm.Unix())
}

func (cn *Conn) SetNetConn(netConn net.Conn) {
	cn.netConn = netConn
	cn.rd.Reset(netConn)
	cn.bw.Reset(netConn)
}

func (cn *Conn) Write(b []byte) (int, error) {
	return cn.netConn.Write(b)
}

func (cn *Conn) RemoteAddr() net.Addr {
	return cn.netConn.RemoteAddr()
}

func (cn *Conn) WithReader(ctx context.Context, timeout time.Duration, fn func(rd *proto.Reader) error) error {
	return internal.WithSpan(ctx, "with_reader", func(ctx context.Context) error {
		if err := cn.netConn.SetReadDeadline(cn.deadline(ctx, timeout)); err != nil {
			return internal.RecordError(ctx, err)
		}
		if err := fn(cn.rd); err != nil {
			return internal.RecordError(ctx, err)
		}
		return nil
	})
}

func (cn *Conn) WithWriter(
	ctx context.Context, timeout time.Duration, fn func(wr *proto.Writer) error,
) error {
	return internal.WithSpan(ctx, "with_writer", func(ctx context.Context) error {
		if err := cn.netConn.SetWriteDeadline(cn.deadline(ctx, timeout)); err != nil {
			return internal.RecordError(ctx, err)
		}

		if cn.bw.Buffered() > 0 {
			cn.bw.Reset(cn.netConn)
		}

		if err := fn(cn.wr); err != nil {
			return internal.RecordError(ctx, err)
		}

		if err := cn.bw.Flush(); err != nil {
			return internal.RecordError(ctx, err)
		}

		internal.WritesCounter.Add(ctx, 1)

		return nil
	})
}

func (cn *Conn) Close() error {
	return cn.netConn.Close()
}

func (cn *Conn) deadline(ctx context.Context, timeout time.Duration) time.Time {
	tm := time.Now()
	cn.SetUsedAt(tm)

	if timeout > 0 {
		tm = tm.Add(timeout)
	}

	if ctx != nil {
		deadline, ok := ctx.Deadline()
		if ok {
			if timeout == 0 {
				return deadline
			}
			if deadline.Before(tm) {
				return deadline
			}
			return tm
		}
	}

	if timeout > 0 {
		return tm
	}

	return noDeadline
}
