package tcpmsg

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"github.com/waitqueue"
	"hash/crc32"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

/* Message header
 * 0        8        16               32
 * ____________________________________
 * |       cmd       |    comprAlg    |
 * ------------------------------------
 * |             length               |
 * ------------------------------------
 * |              crc32               |
 * ------------------------------------
 * |                                  |
 * |             ......               |
 * |                                  |
 * ------------------------------------
 */

type CmdType uint16

const (
	CmdInvalid   CmdType = 0
	CmdKeepalive         = 1
	CmdData              = 2
)

type ComprAlgType uint16

const (
	ComprInvalid ComprAlgType = 0
	ComprNone                 = 1
	ComprZlib                 = 2
)

type msghdr struct {
	conn     *TcpMsgConn
	cmd      CmdType
	comprAlg ComprAlgType
	length   uint32
	crc32    uint32
	data     []byte
}

const msghdrSize int = 12

func (msg *msghdr) compress() error {
	switch msg.comprAlg {
	case ComprNone:
	case ComprZlib:
		outBuf := bytes.NewBuffer(nil)
		if outBuf == nil {
			return fmt.Errorf("bytes.NewBuffer() call failed.")
		}
		w := zlib.NewWriter(outBuf)
		if w == nil {
			return fmt.Errorf("zlib.NewWriter() call failed.")
		}
		_, err := w.Write(msg.data)
		if err != nil {
			w.Close()
			return err
		}
		w.Close()
		msg.data = outBuf.Bytes()
	default:
	}
	return nil
}

func (msg *msghdr) uncompress() error {
	switch msg.comprAlg {
	case ComprNone:
	case ComprZlib:
		rb := bytes.NewBuffer(msg.data)
		if rb == nil {
			return fmt.Errorf("bytes.NewBuffer() call failed.")
		}
		r, err := zlib.NewReader(rb)
		if err != nil {
			return err
		}
		outBuf := bytes.NewBuffer(nil)
		_, err = io.Copy(outBuf, r)
		if err != nil {
			r.Close()
			return err
		}
		r.Close()
		msg.data = outBuf.Bytes()
		return nil
	default:
	}
	return nil
}

func (msg *msghdr) packet() error {
	if msg.data != nil {
		if len(msg.data) > 128 { //数据长度大于128字节的才进行压缩处理后传输，否则不压缩
			msg.comprAlg = ComprZlib
		} else {
			msg.comprAlg = ComprNone
		}
		err := msg.compress()
		if err != nil {
			return err
		}
		msg.length = uint32(len(msg.data))
		msg.crc32 = crc32.ChecksumIEEE(msg.data)
	} else {
		msg.comprAlg = ComprNone
		msg.length = 0
		msg.crc32 = 0
	}
	return nil
}

func (msg *msghdr) unpacket() error {
	if msg.length > 0 {
		crc := crc32.ChecksumIEEE(msg.data)
		if crc != msg.crc32 {
			return fmt.Errorf("msg's crc32 verify failed. recvValue[%x], calValue[%x].", msg.crc32, crc)
		}
		err := msg.uncompress()
		if err != nil {
			return err
		}
	} else {
		if msg.crc32 != 0 {
			return fmt.Errorf("msg's crc32 verify failed. recvValue[%x], calValue[%x].", msg.crc32, 0)
		}
	}
	return nil
}

type TcpMsgReceiveHandler func(conn *TcpMsgConn, data []byte)
type TcpMsgErrorHandler func(conn *TcpMsgConn, err error)

const keepaliveInterval time.Duration = time.Minute            //客户端用的
const keepaliveTimeoutInterval time.Duration = time.Minute * 5 //服务端用的

type TcpMsgConn struct {
	conn              *net.TCPConn
	refCnt            int64
	sendCnt           uint64
	sendBytes         uint64
	sendRealBytes     uint64
	sendErrCnt        uint64
	recvCnt           uint64
	recvBytes         uint64
	recvRealBytes     uint64
	recvErrCnt        uint64
	recvHandler       TcpMsgReceiveHandler
	errHandler        TcpMsgErrorHandler
	exiting           bool
	timerKeepalive    *time.Timer         //客户端用的
	waitItemKeepalive *waitqueue.WaitItem //服务端用的
	server            *TcpMsgServer       //服务端用的
}

func newTcpMsgConn(conn *net.TCPConn, recvHandler TcpMsgReceiveHandler, errHandler TcpMsgErrorHandler) *TcpMsgConn {
	cli := &TcpMsgConn{
		conn:          conn,
		refCnt:        1,
		sendCnt:       0,
		sendBytes:     0,
		sendRealBytes: 0,
		sendErrCnt:    0,
		recvCnt:       0,
		recvBytes:     0,
		recvRealBytes: 0,
		recvErrCnt:    0,
		recvHandler:   recvHandler,
		errHandler:    errHandler,
		exiting:       false,
		server:        nil,
	}
	go cli.receiver()
	return cli
}

func NewTcpMsgClient(serverHost string, recvHandler TcpMsgReceiveHandler, errHandler TcpMsgErrorHandler) (*TcpMsgConn, error) {
	saddr, _ := net.ResolveTCPAddr("tcp", serverHost)
	conn, err := net.DialTCP("tcp", nil, saddr)
	if err != nil {
		return nil, fmt.Errorf("net.DialTCP() failed. errstr[%s].", err.Error())
	}
	tcpMsgConn := newTcpMsgConn(conn, recvHandler, errHandler)
	tcpMsgConn.timerKeepalive = time.AfterFunc(keepaliveInterval, tcpMsgConn.keepalive) //由client端主动发起心跳报文
	return tcpMsgConn, nil
}

func keepaliveTimeout(data interface{}) {
	conn := data.(*TcpMsgConn)
	conn.keepaliveTimeout()
}

func (c *TcpMsgConn) Close() {
	c.exiting = true
	c.put()
}

func (c *TcpMsgConn) get() bool {
	if atomic.AddInt64(&c.refCnt, 1) > 1 {
		return true
	} else {
		return false
	}
}

func (c *TcpMsgConn) put() {
	if atomic.AddInt64(&c.refCnt, -1) == 0 {
		if !c.exiting {
			c.exiting = true
		}
		if c.server != nil {
			c.server.removeConn(c)
		}
		c.conn.Close()
	}
}

func (c *TcpMsgConn) keepalive() {
	if !c.get() {
		return
	}
	defer c.put()
	msg := &msghdr{
		cmd:  CmdKeepalive,
		data: nil,
		conn: c,
	}
	err := msg.packet()
	if err != nil {
		c.timerKeepalive = time.AfterFunc(time.Second, c.keepalive)
		return
	}
	err = c.send(msg)
	if err != nil {
		c.timerKeepalive = time.AfterFunc(time.Second, c.keepalive)
	} else {
		c.timerKeepalive = time.AfterFunc(keepaliveInterval, c.keepalive)
	}
}

func (c *TcpMsgConn) keepaliveReset() {
	if !c.get() {
		return
	}
	defer c.put()
	c.timerKeepalive.Reset(keepaliveInterval)
}

func (c *TcpMsgConn) keepaliveTimeout() {
	c.Close()
}

func (c *TcpMsgConn) keepaliveTimeoutReset() {
	c.waitItemKeepalive.Reset(keepaliveTimeoutInterval)
}

func (c *TcpMsgConn) receiver() {
	c.conn.SetKeepAlive(true)
	c.conn.SetReadBuffer(64 * 1024)
	recvBuf := make([]byte, 64*1024)
	mhBuf := make([]byte, msghdrSize)
	mhRecvLen := 0
	var msg *msghdr = nil
	for !c.exiting {
		if !c.get() {
			break
		}
		c.conn.SetReadDeadline(time.Now().Add(time.Second))
		recvBytes, err := c.conn.Read(recvBuf)
		if err != nil {
			if err == io.EOF { //对端关闭了连接
				if c.errHandler != nil {
					c.errHandler(c, err)
				}
				c.put()
				break
			}
			neterr, ok := err.(net.Error)
			if ok && neterr.Timeout() { //读超时
			} else {
				atomic.AddUint64(&c.recvErrCnt, 1)
			}
		} else {
			data := recvBuf[:]
			dlen := recvBytes
			for dlen > 0 {
				if msg == nil {
					nl := msghdrSize - mhRecvLen
					if nl > 0 && dlen < nl {
						copy(mhBuf[mhRecvLen:], data[0:dlen])
						mhRecvLen += dlen
						break //数据已经拷贝完了，跳出循环
					} else {
						copy(mhBuf[mhRecvLen:], data[0:nl])
						data = data[nl:] //data指针后移
						dlen -= nl
						msg = &msghdr{
							cmd:      CmdType(binary.BigEndian.Uint16(mhBuf[0:2])),
							comprAlg: ComprAlgType(binary.BigEndian.Uint16(mhBuf[2:4])),
							length:   binary.BigEndian.Uint32(mhBuf[4:8]),
							crc32:    binary.BigEndian.Uint32(mhBuf[8:12]),
						}
						mhRecvLen = 0 //reset mhRecvLen
						if msg.length > 0 {
							msg.data = make([]byte, 0)
							cpLen := uint32(dlen)
							if cpLen > msg.length {
								cpLen = msg.length
							}
							msg.data = append(msg.data, data[:cpLen]...)
							if cpLen == msg.length {
								c.handleMsg(msg)
								msg = nil
							}
							data = data[cpLen:] //data指针后移
							dlen -= int(cpLen)
						} else {
							msg.data = nil
							c.handleMsg(msg)
							msg = nil
						}
					}
				} else { //msg != nil
					cpLen := uint32(dlen)
					nl := msg.length - uint32(len(msg.data))
					if cpLen > nl {
						cpLen = nl
					}
					msg.data = append(msg.data, data[:cpLen]...)
					if cpLen == nl {
						c.handleMsg(msg)
						msg = nil
					}
					data = data[cpLen:] //data指针后移
					dlen -= int(cpLen)
				}
			}
		}
		c.put()
	}
}

func (c *TcpMsgConn) handleMsg(msg *msghdr) {
	atomic.AddUint64(&c.recvCnt, 1)
	atomic.AddUint64(&c.recvBytes, uint64(msg.length))
	atomic.AddUint64(&c.recvRealBytes, uint64(msg.length)+uint64(msghdrSize))
	err := msg.unpacket()
	if err != nil {
		fmt.Printf("Unpacket msg failed. errstr[%s].", err.Error())
		return
	}
	if msg.cmd == CmdData && c.recvHandler != nil {
		c.recvHandler(c, msg.data)
	}
}

func (c *TcpMsgConn) send(msg *msghdr) error {
	tc := msghdrSize + int(msg.length)
	sendBuf := make([]byte, tc)
	if sendBuf == nil {
		return fmt.Errorf("Alloc buffer for send data failed.")
	}
	binary.BigEndian.PutUint16(sendBuf[0:2], uint16(msg.cmd))
	binary.BigEndian.PutUint16(sendBuf[2:4], uint16(msg.comprAlg))
	binary.BigEndian.PutUint32(sendBuf[4:8], uint32(msg.length))
	binary.BigEndian.PutUint32(sendBuf[8:12], uint32(msg.crc32))
	if msg.data != nil {
		copy(sendBuf[12:], msg.data)
	}
	sc, err := c.conn.Write(sendBuf)
	if err != nil {
		atomic.AddUint64(&c.sendErrCnt, 1)
	} else {
		atomic.AddUint64(&c.sendCnt, 1)
		atomic.AddUint64(&c.sendRealBytes, uint64(sc))
		atomic.AddUint64(&c.sendBytes, uint64(msg.length))
	}
	return err
}

func (c *TcpMsgConn) Send(data []byte) error {
	if data == nil {
		return fmt.Errorf("Invalid data.")
	}
	dlen := len(data)
	if dlen <= 0 {
		return fmt.Errorf("Invalid data length.")
	}
	if !c.get() {
		return fmt.Errorf("Invalid TcpMsgConn object.")
	}
	defer c.put()
	msg := &msghdr{
		cmd:  CmdData,
		data: data,
		conn: c,
	}
	err := msg.packet()
	if err != nil {
		return err
	}
	return c.send(msg)
}

type TcpMsgServer struct {
	refCnt             int64
	listener           *net.TCPListener
	cliConn            map[string]*TcpMsgConn
	lock               sync.Mutex
	keepaliveWaitqueue *waitqueue.WaitQueue
	recvHandler        TcpMsgReceiveHandler
	errHandler         TcpMsgErrorHandler
	exiting            bool
}

func NewTcpMsgServer(serverHost string, recvHandler TcpMsgReceiveHandler, errHandler TcpMsgErrorHandler) (*TcpMsgServer, error) {
	saddr, _ := net.ResolveTCPAddr("tcp", serverHost)
	if saddr == nil {
		return nil, fmt.Errorf("net.ResolveTCPAddr(\"tcp\", %s) failed.", serverHost)
	}
	tcpListener, _ := net.ListenTCP("tcp", saddr)
	if tcpListener == nil {
		return nil, fmt.Errorf("net.ListenTCP(\"tcp\", %s) failed.", serverHost)
	}
	svr := &TcpMsgServer{
		refCnt:             1,
		listener:           tcpListener,
		cliConn:            make(map[string]*TcpMsgConn),
		keepaliveWaitqueue: waitqueue.NewWaitQueue(1),
		recvHandler:        recvHandler,
		errHandler:         errHandler,
		exiting:            false,
	}
	go svr.accept()
	return svr, nil
}

func (s *TcpMsgServer) Close() {
	s.exiting = true
	s.put()
}

func (s *TcpMsgServer) get() bool {
	if atomic.AddInt64(&s.refCnt, 1) > 1 {
		return true
	} else {
		return false
	}
}

func (s *TcpMsgServer) put() {
	if atomic.AddInt64(&s.refCnt, -1) == 0 {
		s.exiting = true
		s.lock.Lock()
		for _, c := range s.cliConn {
			c.server = nil //必须要在c.Close()之前将c.server设置为nil。避免造成死锁。
			c.Close()
			delete(s.cliConn, c.conn.RemoteAddr().String())
			c.waitItemKeepalive.Remove()
		}
		s.lock.Unlock()
		//s.listener.Close() //此处不用Close，因为在accept方法退出的时候会调用Close。
		s.keepaliveWaitqueue.Destroy()
	}
}

func (s *TcpMsgServer) addConn(conn *TcpMsgConn) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.cliConn[conn.conn.RemoteAddr().String()] = conn
	wi, err := s.keepaliveWaitqueue.Add(conn, keepaliveTimeoutInterval, keepaliveTimeout)
	if err != nil {
		return err
	}
	conn.waitItemKeepalive = wi
	return nil
}

func (s *TcpMsgServer) removeConn(conn *TcpMsgConn) {
	if !s.get() {
		return
	}
	defer s.put()
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.cliConn, conn.conn.RemoteAddr().String())
	conn.waitItemKeepalive.Remove()
}

func (s *TcpMsgServer) accept() {
	defer s.listener.Close()
	for !s.exiting {
		if !s.get() {
			continue
		}
		s.listener.SetDeadline(time.Now().Add(time.Second))
		conn, _ := s.listener.AcceptTCP()
		if conn == nil { //timeout
			s.put()
			continue
		}
		tcpMsgConn := newTcpMsgConn(conn, s.recvHandler, s.errHandler)
		tcpMsgConn.server = s
		err := s.addConn(tcpMsgConn)
		if err != nil {
			fmt.Println("addConn failed. ", err.Error())
		}
		s.put()
	}
}
