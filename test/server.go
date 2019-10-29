package main

import (
	"flag"
	"fmt"
	"github.com/tcpmsg"
	"io"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"
)

var hostaddr string

func init() {
	flag.StringVar(&hostaddr, "h", ":3803", "Host address.")
}

type tcpConnInfo struct {
	conn      *tcpmsg.TcpMsgConn
	recvCount int64
	recvBytes int64
	errCount  int64
}

var tcpConnInfoMap map[*tcpmsg.TcpMsgConn]*tcpConnInfo
var tcpConnInfoMapLock sync.Mutex

func monitor() {
	loopCount := 0
	start := time.Now()
	for {
		time.Sleep(time.Second * 30)
		var connCount int64 = 0
		var recvCount int64 = 0
		var recvBytes int64 = 0
		var errCount int64 = 0
		tcpConnInfoMapLock.Lock()
		loopCount++
		for _, c := range tcpConnInfoMap {
			connCount++
			atomic.AddInt64(&recvCount, c.recvCount)
			atomic.AddInt64(&recvBytes, c.recvBytes)
			atomic.AddInt64(&errCount, c.errCount)
		}
		dur := time.Now().Sub(start).Seconds()
		recvBytesAvg := float64(recvBytes) / dur
		recvAvg := float64(recvCount) / dur
		fmt.Printf("connCount:%d, recvCount:%d, recvBytes:%d, errCount:%d, recvBytesAvg:%f, recvAvg:%f, dur:%f.\r\n", connCount, recvCount, recvBytes, errCount, recvBytesAvg, recvAvg, dur)
		if loopCount%10 == 0 {
			index := 0
			fmt.Println("Show detailed connection information:")
			for k, c := range tcpConnInfoMap {
				index++
				fmt.Printf("%d.\traddr[%s],recvCount[%d],recvBytes[%d].\r\n", index, k, atomic.LoadInt64(&c.recvCount), atomic.LoadInt64(&c.recvBytes))
			}
			fmt.Println("-------------------------------------")
		}
		tcpConnInfoMapLock.Unlock()
	}
}

func recvHandler(conn *tcpmsg.TcpMsgConn, data []byte) {
	n := len(data)
	tcpConnInfoMapLock.Lock()
	c, ok := tcpConnInfoMap[conn]
	if !ok {
		c = &tcpConnInfo{
			conn:      conn,
			recvCount: 0,
			recvBytes: 0,
		}
		tcpConnInfoMap[conn] = c
	}
	tcpConnInfoMapLock.Unlock()
	if n != 64 {
		atomic.AddInt64(&c.errCount, 1)
	} else {
		atomic.AddInt64(&c.recvCount, 1)
		atomic.AddInt64(&c.recvBytes, int64(n))
	}
}

func errHandler(conn *tcpmsg.TcpMsgConn, err error) {
	if err == io.EOF {
		tcpConnInfoMapLock.Lock()
		delete(tcpConnInfoMap, conn)
		tcpConnInfoMapLock.Unlock()
	}
}

func main() {
	flag.Parse()
	tcpConnInfoMap = make(map[*tcpmsg.TcpMsgConn]*tcpConnInfo)
	svr, err := tcpmsg.NewTcpMsgServer(hostaddr, recvHandler, errHandler)
	if err != nil {
		panic(err.Error())
		return
	}
	go monitor()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	s := <-c
	fmt.Println("Got signal:", s)
	svr.Close()
}
