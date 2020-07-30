package pool

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8/internal"
)

var (
	ErrClosed      = errors.New("redis: client is closed")
	ErrPoolTimeout = errors.New("redis: connection pool timeout")
)

var timers = sync.Pool{
	New: func() interface{} {
		t := time.NewTimer(time.Hour)
		t.Stop()
		return t
	},
}

// Stats contains pool state information and accumulated stats.
type Stats struct {
	Hits     uint32 // number of times free connection was found in the pool
	Misses   uint32 // number of times free connection was NOT found in the pool
	Timeouts uint32 // number of times a wait timeout occurred

	TotalConns uint32 // number of total connections in the pool
	IdleConns  uint32 // number of idle connections in the pool
	StaleConns uint32 // number of stale connections removed from the pool
}

type Pooler interface {
	NewConn(context.Context) (*Conn, error) //创建连接
	CloseConn(*Conn) error                  //关闭连接

	Get(context.Context) (*Conn, error) //获取连接
	Put(*Conn)                          //放回连接
	Remove(*Conn, error)                //移除连接

	Len() int      //连接池长度
	IdleLen() int  //空闲连接池长度
	Stats() *Stats //连接池状态获取

	Close() error //关闭连接池
}

type Options struct {
	Dialer  func(context.Context) (net.Conn, error)
	OnClose func(*Conn) error

	PoolSize           int           //连接池大小
	MinIdleConns       int           //最小的空闲连接数
	MaxConnAge         time.Duration //客户端关闭连接的时间
	PoolTimeout        time.Duration //连接池超时时间
	IdleTimeout        time.Duration //空闲连接闲置超过该事件 将被关闭
	IdleCheckFrequency time.Duration //进行空闲检查的时间
}

type lastDialErrorWrap struct {
	err error
}

//核心连接池结构体
type ConnPool struct {
	opt *Options //连接池配置

	dialErrorsNum uint32 // atomic 连接池错误次数

	lastDialError atomic.Value //上一次连接错误

	queue chan struct{} //工作连接队列

	connsMu      sync.Mutex //连接队列锁
	conns        []*Conn    //连接切片
	idleConns    []*Conn    //空闲连接切片
	poolSize     int        //连接池大小
	idleConnsLen int        //空闲连接大小

	stats Stats //连接池状态统计

	_closed  uint32        // atomic连接池状态
	closedCh chan struct{} //连接池关闭通道
}

var _ Pooler = (*ConnPool)(nil)

func NewConnPool(opt *Options) *ConnPool {
	p := &ConnPool{
		opt: opt,

		queue:     make(chan struct{}, opt.PoolSize),
		conns:     make([]*Conn, 0, opt.PoolSize),
		idleConns: make([]*Conn, 0, opt.PoolSize),
		closedCh:  make(chan struct{}),
	}

	//检查最小连接数 如果最小连接数不足 新建连接
	p.connsMu.Lock()
	p.checkMinIdleConns()
	p.connsMu.Unlock()

	//等待空闲连接超时 +　空闲连接检查　均设置的时候开启一个清理空闲连接的go程
	if opt.IdleTimeout > 0 && opt.IdleCheckFrequency > 0 {
		go p.reaper(opt.IdleCheckFrequency)
	}

	return p
}

func (p *ConnPool) checkMinIdleConns() {
	if p.opt.MinIdleConns == 0 {
		return
	}
	//如果当前连接小于连接池的连接数 空闲连接小于最小空闲连接 通过go程新建连接
	for p.poolSize < p.opt.PoolSize && p.idleConnsLen < p.opt.MinIdleConns {
		p.poolSize++     //连接数
		p.idleConnsLen++ //空闲连接数
		go func() {
			//添加空闲连接
			err := p.addIdleConn()
			if err != nil {
				p.connsMu.Lock()
				p.poolSize--
				p.idleConnsLen--
				p.connsMu.Unlock()
			}
		}()
	}
}

func (p *ConnPool) addIdleConn() error {
	cn, err := p.dialConn(context.TODO(), true)
	if err != nil {
		return err
	}

	//连接池和空闲连接池均添加该连接
	p.connsMu.Lock()
	p.conns = append(p.conns, cn)
	p.idleConns = append(p.idleConns, cn)
	p.connsMu.Unlock()
	return nil
}

func (p *ConnPool) NewConn(ctx context.Context) (*Conn, error) {
	return p.newConn(ctx, false)
}

func (p *ConnPool) newConn(ctx context.Context, pooled bool) (*Conn, error) {
	cn, err := p.dialConn(ctx, pooled)
	if err != nil {
		return nil, err
	}

	p.connsMu.Lock()
	p.conns = append(p.conns, cn)
	if pooled {
		// If pool is full remove the cn on next Put.
		if p.poolSize >= p.opt.PoolSize {
			cn.pooled = false
		} else {
			p.poolSize++
		}
	}
	p.connsMu.Unlock()
	return cn, nil
}

func (p *ConnPool) dialConn(ctx context.Context, pooled bool) (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	if atomic.LoadUint32(&p.dialErrorsNum) >= uint32(p.opt.PoolSize) {
		return nil, p.getLastDialError()
	}

	netConn, err := p.opt.Dialer(ctx)
	if err != nil {
		p.setLastDialError(err)
		if atomic.AddUint32(&p.dialErrorsNum, 1) == uint32(p.opt.PoolSize) {
			go p.tryDial()
		}
		return nil, err
	}

	internal.NewConnectionsCounter.Add(ctx, 1)
	cn := NewConn(netConn)
	cn.pooled = pooled
	return cn, nil
}

func (p *ConnPool) tryDial() {
	for {
		if p.closed() {
			return
		}

		conn, err := p.opt.Dialer(context.Background())
		if err != nil {
			p.setLastDialError(err)
			time.Sleep(time.Second)
			continue
		}

		atomic.StoreUint32(&p.dialErrorsNum, 0)
		_ = conn.Close()
		return
	}
}

func (p *ConnPool) setLastDialError(err error) {
	p.lastDialError.Store(&lastDialErrorWrap{err: err})
}

func (p *ConnPool) getLastDialError() error {
	err, _ := p.lastDialError.Load().(*lastDialErrorWrap)
	if err != nil {
		return err.err
	}
	return nil
}

// Get returns existed connection from the pool or creates a new one.
func (p *ConnPool) Get(ctx context.Context) (*Conn, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	//todo 该逻辑后续再度
	err := p.waitTurn(ctx)
	if err != nil {
		return nil, err
	}

	for {
		//获取连接
		p.connsMu.Lock()
		cn := p.popIdle()
		p.connsMu.Unlock()

		if cn == nil {
			break
		}

		//判断是否是无效连接 如果是无效连接则继续变流
		if p.isStaleConn(cn) {
			_ = p.CloseConn(cn)
			continue
		}

		//统计计数 获取连接命中数+1
		atomic.AddUint32(&p.stats.Hits, 1)
		return cn, nil
	}

	//统计计数 未获取连接数+1
	atomic.AddUint32(&p.stats.Misses, 1)

	//新建连接 并直接添加至连接池
	newcn, err := p.newConn(ctx, true)
	if err != nil {
		p.freeTurn()
		return nil, err
	}

	return newcn, nil
}

func (p *ConnPool) getTurn() {
	p.queue <- struct{}{}
}

func (p *ConnPool) waitTurn(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	select {
	case p.queue <- struct{}{}:
		return nil
	default:
	}

	timer := timers.Get().(*time.Timer)
	timer.Reset(p.opt.PoolTimeout)

	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return ctx.Err()
	case p.queue <- struct{}{}:
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return nil
	case <-timer.C:
		timers.Put(timer)
		atomic.AddUint32(&p.stats.Timeouts, 1)
		return ErrPoolTimeout
	}
}

func (p *ConnPool) freeTurn() {
	<-p.queue
}

func (p *ConnPool) popIdle() *Conn {
	if len(p.idleConns) == 0 {
		return nil
	}

	idx := len(p.idleConns) - 1
	cn := p.idleConns[idx]
	p.idleConns = p.idleConns[:idx]
	p.idleConnsLen--
	p.checkMinIdleConns()
	return cn
}

func (p *ConnPool) Put(cn *Conn) {
	//判断连接是否还有数据未被读取 如果有移除连接 并返回响应错误
	if cn.rd.Buffered() > 0 {
		internal.Logger.Printf(context.Background(), "Conn has unread data")
		p.Remove(cn, BadConnError{})
		return
	}

	//如果连接未放入连接池 则直接移除
	if !cn.pooled {
		p.Remove(cn, nil)
		return
	}

	//将连接放回到空闲队列  并对队列计数+1
	p.connsMu.Lock()
	p.idleConns = append(p.idleConns, cn)
	p.idleConnsLen++
	p.connsMu.Unlock()
	p.freeTurn()
}

func (p *ConnPool) Remove(cn *Conn, reason error) {
	p.removeConnWithLock(cn)
	p.freeTurn()
	_ = p.closeConn(cn)
}

func (p *ConnPool) CloseConn(cn *Conn) error {
	p.removeConnWithLock(cn)
	return p.closeConn(cn)
}

func (p *ConnPool) removeConnWithLock(cn *Conn) {
	p.connsMu.Lock()
	p.removeConn(cn)
	p.connsMu.Unlock()
}

func (p *ConnPool) removeConn(cn *Conn) {
	//遍历队列 找到要关闭的连接
	for i, c := range p.conns {
		if c == cn {
			//重新拼接连接切片
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			//如果是连接池中
			if cn.pooled {
				p.poolSize--
				//检查最小空余连接数量
				p.checkMinIdleConns()
			}
			return
		}
	}
}

func (p *ConnPool) closeConn(cn *Conn) error {
	//先执行关闭连接的回调函数 在关闭连接
	if p.opt.OnClose != nil {
		_ = p.opt.OnClose(cn)
	}
	return cn.Close()
}

// Len returns total number of connections.
func (p *ConnPool) Len() int {
	p.connsMu.Lock()
	n := len(p.conns)
	p.connsMu.Unlock()
	return n
}

// IdleLen returns number of idle connections.
func (p *ConnPool) IdleLen() int {
	p.connsMu.Lock()
	n := p.idleConnsLen
	p.connsMu.Unlock()
	return n
}

//获取连接池状态
func (p *ConnPool) Stats() *Stats {
	idleLen := p.IdleLen()
	return &Stats{
		Hits:     atomic.LoadUint32(&p.stats.Hits),
		Misses:   atomic.LoadUint32(&p.stats.Misses),
		Timeouts: atomic.LoadUint32(&p.stats.Timeouts),

		TotalConns: uint32(p.Len()),
		IdleConns:  uint32(idleLen),
		StaleConns: atomic.LoadUint32(&p.stats.StaleConns),
	}
}

func (p *ConnPool) closed() bool {
	return atomic.LoadUint32(&p._closed) == 1
}

func (p *ConnPool) Filter(fn func(*Conn) bool) error {
	var firstErr error
	p.connsMu.Lock()
	for _, cn := range p.conns {
		//对所有满足条件的连接进行关闭
		if fn(cn) {
			if err := p.closeConn(cn); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	p.connsMu.Unlock()
	return firstErr
}

func (p *ConnPool) Close() error {
	//检查关闭标注
	if !atomic.CompareAndSwapUint32(&p._closed, 0, 1) {
		return ErrClosed
	}
	//关闭通道，连接池中的所有协程都可以通过判断该通道是否关闭来确定连接池是否已经关闭
	close(p.closedCh)

	var firstErr error
	//遍历关闭所有连接
	p.connsMu.Lock()
	for _, cn := range p.conns {
		if err := p.closeConn(cn); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.conns = nil
	p.poolSize = 0
	p.idleConns = nil
	p.idleConnsLen = 0
	p.connsMu.Unlock()

	return firstErr
}

func (p *ConnPool) reaper(frequency time.Duration) {
	ticker := time.NewTicker(frequency)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C: //定时器触发
			// It is possible that ticker and closedCh arrive together,
			// and select pseudo-randomly pick ticker case, we double
			// check here to prevent being executed after closed.
			if p.closed() {
				return
			}
			_, err := p.ReapStaleConns() //清理过期的连接
			if err != nil {
				internal.Logger.Printf(context.Background(), "ReapStaleConns failed: %s", err)
				continue
			}
		case <-p.closedCh: //连接池关闭触发
			return
		}
	}
}

func (p *ConnPool) ReapStaleConns() (int, error) {
	var n int
	//无限循环 直到所有过期连接均被清理
	for {
		p.getTurn()

		//取出过期连接 永远只取idleConns[0] 如果该连接有效，则证明后续连接都未超时
		p.connsMu.Lock()
		cn := p.reapStaleConn()
		p.connsMu.Unlock()
		p.freeTurn()

		if cn != nil {
			_ = p.closeConn(cn) //关闭过期连接
			n++
		} else {
			break
		}
	}
	atomic.AddUint32(&p.stats.StaleConns, uint32(n))
	return n, nil
}

func (p *ConnPool) reapStaleConn() *Conn {
	if len(p.idleConns) == 0 {
		return nil
	}

	//取出空闲连接，判断是否为过期连接
	cn := p.idleConns[0]
	if !p.isStaleConn(cn) {
		return nil
	}

	p.idleConns = append(p.idleConns[:0], p.idleConns[1:]...)
	p.idleConnsLen--
	p.removeConn(cn)

	return cn
}

func (p *ConnPool) isStaleConn(cn *Conn) bool {
	if p.opt.IdleTimeout == 0 && p.opt.MaxConnAge == 0 {
		return false
	}

	now := time.Now()
	//连接从上次使用之后的空闲时间大于闲置超时时间时就会被关闭
	if p.opt.IdleTimeout > 0 && now.Sub(cn.UsedAt()) >= p.opt.IdleTimeout {
		return true
	}
	//连接超过最大连接使用时间 就会被关闭
	if p.opt.MaxConnAge > 0 && now.Sub(cn.createdAt) >= p.opt.MaxConnAge {
		return true
	}

	return false
}
