package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

const SockAddr = "/tmp/rpc.sock"

type RpcClient struct {
	kafka *KafkaClient
}

func NewRpcClient() (RpcClient, error) {
	greeter := RpcClient{}
	k, err := NewKafkaClient()
	greeter.kafka = k

	return greeter, err	
}

func (g RpcClient) SendKafka(name *string, reply *string) error {
	err := g.kafka.Send(name, reply)

	return err
}

func main() {
	if err := os.RemoveAll(SockAddr); err != nil {
		log.Fatal(err)
	}

	greeter, err := NewRpcClient()
	if err != nil {
		log.Fatal(err)
	}
	
	if err := rpc.Register(greeter); err != nil {
		log.Fatal(err)
	}

	rpc.HandleHTTP()
	l, e := net.Listen("unix", SockAddr)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	fmt.Println("Serving...")
	http.Serve(l, nil)
}
