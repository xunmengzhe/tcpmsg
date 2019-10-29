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

var serveraddr string

func init() {
	flag.StringVar(&serveraddr, "s", "10.1.2.12:3803", "Server address.")
}

type clientInfo struct {
	index     int64
	sendCount int64
	sendBytes int64
	errCount  int64
}

var clientInfoArray [10]*clientInfo
var clientInfoArrayLock sync.Mutex
var exitClientSendCount int64 = 0
var exitClientSendBytes int64 = 0
var exitClientErrCount int64 = 0
var runningCount int = 0
var exitCount int = 0
var totalCount int = 0

func clientHandler(index int64) {
	conn, err := tcpmsg.NewTcpMsgClient(serveraddr, nil, nil)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer conn.Close()
	data := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-=")
	info := &clientInfo{
		index:     index,
		sendCount: 0,
		sendBytes: 0,
		errCount:  0,
	}
	clientInfoArrayLock.Lock()
	clientInfoArray[info.index] = info
	runningCount++
	totalCount++
	clientInfoArrayLock.Unlock()
	for {
		err := conn.Send([]byte(data))
		if err == nil {
			atomic.AddInt64(&info.sendCount, 1)
			atomic.AddInt64(&info.sendBytes, int64(len(data)))
		} else {
			atomic.AddInt64(&info.errCount, 1)
			if err == io.EOF { //连接断开
				break
			}
		}
	}
	clientInfoArrayLock.Lock()
	clientInfoArray[info.index] = nil
	exitClientSendCount += info.sendCount
	exitClientSendBytes += info.sendBytes
	exitClientErrCount += info.errCount
	exitCount++
	runningCount--
	clientInfoArrayLock.Unlock()
}

func monitor() {
	loopCount := 0
	start := time.Now()
	for {
		time.Sleep(time.Second * 30)
		var sendCount int64 = 0
		var sendBytes int64 = 0
		var errCount int64 = 0
		clientInfoArrayLock.Lock()
		loopCount++
		for _, c := range clientInfoArray {
			if c != nil {
				atomic.AddInt64(&sendCount, c.sendCount)
				atomic.AddInt64(&sendBytes, c.sendBytes)
				atomic.AddInt64(&errCount, c.errCount)
			}
		}
		sendCount += exitClientSendCount
		sendBytes += exitClientSendBytes
		errCount += exitClientErrCount
		dur := time.Now().Sub(start).Seconds()
		sendBytesAvg := float64(sendBytes) / dur
		sendAvg := float64(sendCount) / dur
		errAvg := float64(errCount) / dur
		fmt.Printf("totalCount:%d, runningCount:%d, exitCount:%d, exitClientSendCount:%d, exitClientSendBytes:%d, exitClientErrCount:%d, sendCount:%d, sendBytes:%d, errCount:%d, sendBytesAvg:%f, sendAvg:%f, errAvg:%f, dur:%f.\r\n",
			totalCount, runningCount, exitCount, exitClientSendCount, exitClientSendBytes, exitClientErrCount, sendCount, sendBytes, errCount, sendBytesAvg, sendAvg, errAvg, dur)
		if loopCount%10 == 0 {
			index := 0
			fmt.Println("Show detailed running client information:")
			for _, c := range clientInfoArray {
				if c != nil {
					index++
					fmt.Printf("%d.\tsendCount[%d],sendBytes[%d],errCount[%d].\r\n", index, atomic.LoadInt64(&c.sendCount), atomic.LoadInt64(&c.sendBytes), atomic.LoadInt64(&c.errCount))
				}
			}
			fmt.Println("-------------------------------------")
		}
		clientInfoArrayLock.Unlock()
	}
}

func clients() {
	for index := 0; index < len(clientInfoArray); index++ {
		go clientHandler(int64(index))
	}
}

func main() {
	flag.Parse()
	go clients()
	go monitor()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	s := <-c
	fmt.Println("Got signal:", s)
}
