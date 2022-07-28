package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"
)

type Delegate struct {
	Value int32
	Port  int
}

func (d *Delegate) NodeMeta(limit int) []byte {
	log.Printf("NodeMeta called")
	return []byte(fmt.Sprintf("localhost:%d:%d", d.Port, atomic.LoadInt32(&d.Value)))
}

func (d Delegate) NotifyMsg([]byte) {
	log.Printf("NotifyMsg called")
}

func (d Delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

func (d Delegate) LocalState(join bool) []byte {
	log.Printf("LocalState called")
	return []byte(fmt.Sprintf("port=%d", d.Port))
}

func (d Delegate) MergeRemoteState(buf []byte, join bool) {
	log.Printf("MergeRemoteState called")
}

func main() {
	listenPort := flag.Int("port", 0, "listening port")
	bootstrapAddr := flag.String("join", "", "list of addresses to join")
	flag.Parse()

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("get hostname: %q", err)
	}

	cfg := memberlist.DefaultLocalConfig()

	delegate := &Delegate{Port: *listenPort}
	cfg.Delegate = delegate
	cfg.Name = fmt.Sprintf("%s:%d", hostname, *listenPort)
	cfg.BindAddr = "127.0.0.1"
	cfg.BindPort = *listenPort
	cfg.AdvertiseAddr = cfg.BindAddr
	cfg.AdvertisePort = cfg.BindPort
	list, err := memberlist.Create(cfg)
	if err != nil {
		log.Fatalf("create list: %q", err)
	}

	if *bootstrapAddr != "" {
		if _, err := list.Join(strings.Split(*bootstrapAddr, ",")); err != nil {
			log.Fatal("join: %q", err)
		}
	}

	for range time.NewTicker(5 * time.Second).C {
		for _, member := range list.Members() {
			log.Printf("%q: %q", member.Name, member.Meta)
		}

		list.UpdateNode(time.Second)
		atomic.AddInt32(&delegate.Value, 1)
	}
}
