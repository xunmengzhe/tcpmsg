package tcpmsg

import (
	"fmt"
	"io"
	"testing"
	"time"
)

const svrHost string = "localhost:3803"

func TestTcpMsg(t *testing.T) {
	go testServer()
	time.Sleep(time.Second)
	testClient()
	time.Sleep(time.Second * 10)
}

func testServerRecvFunc(conn *TcpMsgConn, data []byte) {
	fmt.Println("testServerRecvFunc: [", conn.conn.RemoteAddr().String(), "]=>[", string(data), "].")
	replyData := fmt.Sprintf("Reply => %s", string(data))
	conn.Send([]byte(replyData))
	//time.Sleep(time.Millisecond * 100)
	//conn.Close()
}

func testServerErrFunc(conn *TcpMsgConn, err error) {
	fmt.Println("testServerErrFunc: [", conn.conn.RemoteAddr().String(), "]=>[", err.Error(), "].")
	if err == io.EOF {
		conn.Close()
	}
}

func testServer() {
	svr, err := NewTcpMsgServer(svrHost, testServerRecvFunc, testServerErrFunc)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer svr.Close()
	time.Sleep(5 * time.Second)
}

func testClientRecvFunc(conn *TcpMsgConn, data []byte) {
	fmt.Println("testClientRecvFunc: [", conn.conn.RemoteAddr().String(), "]=>[", string(data), "].")
}

func testClientErrFunc(conn *TcpMsgConn, err error) {
	fmt.Println("testClientErrFunc: [", conn.conn.RemoteAddr().String(), "]=>[", err.Error(), "].")
}

func testClient2(index int) {
	cli, err := NewTcpMsgClient(svrHost, testClientRecvFunc, testClientErrFunc)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer cli.Close()
	data := fmt.Sprintf("%d. Hello World!", index)
	err = cli.Send([]byte(data))
	if err != nil {
		return
	}
	time.Sleep(time.Second)
}

func testClient() {
	index := 1
	for index = 1; index <= 10; index++ {
		go testClient2(index)
	}
}
